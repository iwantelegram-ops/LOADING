"""
core/mongo_shard.py — Multi-Cluster MongoDB Shard Manager
──────────────────────────────────────────────────────────
LATAR BELAKANG:
  MongoDB free tier (M0) dibatasi ~100 operasi/detik. Saat bot berjalan
  di banyak grup ramai, collection yang ditulis PER EVENT per grup
  (bio_profiles, seen_messages, mention_member_cache, dst) bisa jauh
  melebihi limit itu sendirian → tulisan telat / throttled / timeout.

STRATEGI:
  Sediakan beberapa cluster Mongo gratis sekaligus (MONGO_URL, MONGO_URL_2,
  MONGO_URL_3, ... tanpa batas di .env). Setiap grup (chat_id) di-hash
  konsisten ke SATU cluster — semua collection "per-grup" milik grup itu
  (bio_profiles, seen_messages, dll) akan selalu mendarat di cluster yang
  sama. Ini membagi beban tulis secara horizontal antar cluster, bukan
  jadi satu jalur 100 ops/detik untuk semua grup.

  Collection yang BUKAN per-grup (config global, regex_list global, dst —
  tidak ada field chat_id yang relevan untuk sharding) tetap di cluster
  index 0 (cluster "utama") saja. Tidak proporsional untuk di-shard dan
  volumenya jauh lebih kecil daripada bio_profiles/seen_messages.

  Setiap cluster independen: kalau salah satu cluster down/lambat, hanya
  grup-grup yang ter-assign ke cluster itu yang fallback ke SQLite lokal
  (per-shard) — grup lain yang ter-assign ke cluster sehat tetap normal.
  Ini BEDA dari skema lama (1 MONGO_URL down → SEMUA grup fallback SQLite).

PEMAKAIAN .env:
  MONGO_URL=mongodb+srv://cluster1...
  MONGO_URL_2=mongodb+srv://cluster2...
  MONGO_URL_3=mongodb+srv://cluster3...
  (boleh terus ditambah, auto-terdeteksi berurutan tanpa ubah kode)

  Jika hanya MONGO_URL diisi → perilaku identik dengan sebelumnya
  (1 cluster, tidak ada perubahan apapun untuk yang belum upgrade).

TIDAK ADA CALLER YANG PERLU DIUBAH:
  Modul ini hanya dipakai secara internal oleh database.py. Semua kode
  lain (plugins/, core/, security_os/) tetap memanggil db["collection"]
  seperti biasa — sharding terjadi transparan di belakang.
"""

from __future__ import annotations

import os
import zlib
import asyncio
from pathlib import Path as _Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=_Path(__file__).resolve().parent.parent / ".env", override=False)

# ── Collection yang di-shard per-grup (volume tulis tinggi, punya chat_id) ────
# Field "chat_id" di query/doc dipakai sebagai hash key penentu shard.
# Tambahkan nama collection lain di sini jika ke depannya ada collection
# baru yang ditulis per-event per-grup.
SHARDED_COLLECTIONS: set[str] = {
    "bio_profiles",
    "seen_messages",
    "mention_member_cache",
    "group_action_log",
    "nexus_actlog",
    "ubot_sentence_tracker",
}


def _collect_mongo_urls() -> list[str]:
    """
    Kumpulkan semua MONGO_URL, MONGO_URL_2, MONGO_URL_3, ... dari .env
    secara berurutan tanpa batas. Berhenti saat menemukan nomor yang
    tidak diisi (mencegah lubang di tengah penomoran yang membingungkan).
    """
    urls: list[str] = []
    first = os.environ.get("MONGO_URL", "").strip()
    if first:
        urls.append(first)
    n = 2
    while True:
        val = os.environ.get(f"MONGO_URL_{n}", "").strip()
        if not val:
            break
        urls.append(val)
        n += 1
    return urls


MONGO_URLS: list[str] = _collect_mongo_urls()
SHARD_COUNT: int = len(MONGO_URLS)


def shard_index_for_chat(chat_id: int) -> int:
    """
    Hash konsisten chat_id → index shard (0..SHARD_COUNT-1).
    crc32 dipakai (bukan hash() bawaan Python) karena hash() di-randomize
    per-proses oleh PYTHONHASHSEED — crc32 selalu deterministik di semua
    proses & restart, sehingga grup yang sama SELALU jatuh ke shard yang
    sama walau bot di-redeploy.
    """
    if SHARD_COUNT <= 1:
        return 0
    key = str(chat_id).encode("utf-8")
    return zlib.crc32(key) % SHARD_COUNT


def extract_chat_id(query_or_doc: dict) -> int | None:
    """
    Coba ambil chat_id dari dict query/document MongoDB.
    Menangani bentuk filter sederhana {"chat_id": 123} maupun
    {"chat_id": {"$eq": 123}} (jarang dipakai di codebase ini, tapi aman).
    """
    if not isinstance(query_or_doc, dict):
        return None
    val = query_or_doc.get("chat_id")
    if isinstance(val, dict):
        val = val.get("$eq")
    if isinstance(val, (int,)):
        return val
    if isinstance(val, str):
        try:
            return int(val)
        except ValueError:
            return None
    return None


# ── Pool client per-shard (lazy, diisi oleh database.py saat _init_backend) ──
# Key: shard_index, Value: motor database object (atau None jika shard itu
# gagal konek dan harus fallback SQLite lokal khusus shard tersebut).
_shard_dbs: dict[int, object] = {}
_shard_healthy: dict[int, bool] = {}
_lock = asyncio.Lock()


def set_shard_db(idx: int, mongo_db_obj) -> None:
    _shard_dbs[idx] = mongo_db_obj
    _shard_healthy[idx] = mongo_db_obj is not None


def get_shard_db(idx: int):
    """Return motor database object untuk shard idx, atau None jika shard itu down."""
    return _shard_dbs.get(idx)


def is_shard_healthy(idx: int) -> bool:
    return _shard_healthy.get(idx, False)


def mark_shard_down(idx: int) -> None:
    """Dipanggil saat satu operasi ke shard idx gagal — supaya caller bisa
    fallback ke SQLite shard tersebut tanpa menunggu timeout berulang-ulang
    untuk tiap request berikutnya dalam waktu dekat."""
    _shard_healthy[idx] = False


def shard_summary() -> str:
    """Ringkasan status semua shard untuk log startup."""
    if SHARD_COUNT == 0:
        return "tidak ada MONGO_URL — full SQLite"
    parts = []
    for i in range(SHARD_COUNT):
        status = "OK" if _shard_healthy.get(i) else "DOWN/SQLite-fallback"
        parts.append(f"shard{i}={status}")
    return ", ".join(parts)
