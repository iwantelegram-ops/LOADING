"""
core/sticker_guard.py
──────────────────────
Sistem pelaporan & blokir stiker pack global.

Dipakai oleh:
  • plugins/commands/reportsticker.py — perintah member /reportsticker
    (reply stiker di grup) yang menambah hitungan laporan.
  • plugins/filters/sticker_guard_filter.py — penegakan otomatis di latar
    belakang: setiap stiker masuk dicek terhadap cache blokir, dihapus +
    memicu hukuman eskalasi jika cocok.
  • plugins/commands/stickerpack_owner.py — /cekstickerpack & /openstikerpack
    (owner DM) untuk melihat & membuka pack dari daftar blokir.

CARA KERJA:
  • 1 user hanya dihitung SEKALI per pack (lihat field "reporters") — lapor
    ulang oleh user yang sama tidak menambah hitungan.
  • Saat hitungan pelapor unik mencapai REPORT_THRESHOLD → pack ditandai
    blacklisted=True secara permanen sampai dibuka manual oleh owner.
  • Judul pack (set_title) hanya diambil via API SEKALI, saat pack pertama
    kali dilaporkan (dokumen belum ada di DB). Laporan berikutnya untuk pack
    yang sama tidak memanggil API apapun — cukup baca dokumen yang sudah ada.
  • Cache in-memory ber-TTL dipakai supaya filter background (jalan di
    SETIAP pesan stiker, semua grup) tidak query DB setiap kali — mengikuti
    pola CONFIG_TTL/ADMIN_TTL yang sudah ada di proyek ini.

DATABASE: collection "sticker_report" (global, tidak di-shard per grup).
  _id / set_name   = nama pendek pack (message.sticker.set_name)
  set_title        = judul pack (hasil GetStickerSet, diisi sekali)
  count            = jumlah pelapor unik
  reporters        = list user_id yang sudah melapor pack ini
  sample_file_id   = file_id stiker contoh, untuk preview owner
  blacklisted      = bool
  blacklisted_at   = timestamp epoch saat masuk blokir (None jika belum)
  created_at / updated_at = timestamp epoch
"""

import os
import time
import asyncio

from database import db

REPORT_THRESHOLD     = int(os.environ.get("STICKER_REPORT_THRESHOLD", 5))
_BLACKLIST_CACHE_TTL = 60.0  # detik

sticker_report_db = db["sticker_report"]

_blacklist_cache:    set[str] | None = None
_blacklist_cache_ts: float          = 0.0
_cache_lock = asyncio.Lock()


async def _refresh_blacklist_cache(force: bool = False) -> set[str]:
    """Muat ulang set nama pack yang diblokir dari DB. Aman dipanggil
    bersamaan (lock) — pemanggil ganda saat cache basi cukup tunggu 1 query."""
    global _blacklist_cache, _blacklist_cache_ts

    now = time.monotonic()
    if not force and _blacklist_cache is not None and (now - _blacklist_cache_ts) < _BLACKLIST_CACHE_TTL:
        return _blacklist_cache

    async with _cache_lock:
        now = time.monotonic()
        if not force and _blacklist_cache is not None and (now - _blacklist_cache_ts) < _BLACKLIST_CACHE_TTL:
            return _blacklist_cache

        try:
            docs  = await sticker_report_db.find({"blacklisted": True}).to_list()
            names = {d["_id"] for d in docs}
        except Exception as e:
            print(f"[StickerGuard] ⚠️  Gagal refresh cache blokir: {e}")
            return _blacklist_cache or set()

        _blacklist_cache    = names
        _blacklist_cache_ts = now
        return names


async def is_blacklisted(set_name: str | None) -> bool:
    """True jika `set_name` ada di daftar blokir global (pakai cache TTL)."""
    if not set_name:
        return False
    cache = await _refresh_blacklist_cache()
    return set_name in cache


async def has_reported(set_name: str, user_id: int) -> bool:
    """True jika `user_id` sudah pernah melaporkan pack `set_name` ini."""
    doc = await sticker_report_db.find_one({"_id": set_name})
    if not doc:
        return False
    return user_id in doc.get("reporters", [])


async def record_report(
    set_name: str, set_title: str, user_id: int, sample_file_id: str | None
) -> tuple[int, bool]:
    """
    Catat 1 laporan baru untuk `set_name` dari `user_id`.

    PENTING — wrapper `database.Collection` di proyek ini (unified
    Mongo/SQLite) TIDAK punya `find_one_and_update`, cuma `find_one` +
    `update_one` (lihat database.py). Jadi fungsi ini ikut pola yang sudah
    dipakai & terbukti jalan di proyek ini (mis. mention_wl_add di
    database.py): upsert dulu kalau dokumen belum ada, lalu `update_one`
    dengan `$addToSet` (atomik di level MongoDB asli — wrapper ini cuma
    forward apa adanya ke col.update_one untuk backend mongo), baru baca
    ulang hasilnya lewat `find_one`.

    Race window antara `update_one` (atomik) dan `find_one` (baca ulang)
    SANGAT kecil dan tidak menyebabkan laporan hilang — yang atomik
    (penambahan ke `reporters`) tetap di level $addToSet itu sendiri.

    Return (count_terbaru, baru_diblokir_sekarang: bool).
    """
    now = time.time()

    existing = await sticker_report_db.find_one({"_id": set_name})
    if existing is None:
        await sticker_report_db.update_one(
            {"_id": set_name},
            {"$set": {
                "_id":            set_name,
                "set_name":       set_name,
                "set_title":      set_title or set_name,
                "sample_file_id": sample_file_id,
                "blacklisted":    False,
                "blacklisted_at": None,
                "created_at":     now,
                "reporters":      [],
                "count":          0,
            }},
            upsert=True,
        )

    # ── Tambah reporter — $addToSet mencegah duplikat di level DB juga ──────
    await sticker_report_db.update_one(
        {"_id": set_name},
        {"$addToSet": {"reporters": user_id}, "$set": {"updated_at": now}},
    )

    doc   = await sticker_report_db.find_one({"_id": set_name}) or {}
    count = len(doc.get("reporters", []))
    if doc.get("count") != count:
        await sticker_report_db.update_one({"_id": set_name}, {"$set": {"count": count}})

    became_blacklisted = False
    if count >= REPORT_THRESHOLD and not doc.get("blacklisted"):
        result = await sticker_report_db.update_one(
            {"_id": set_name, "blacklisted": False},
            {"$set": {"blacklisted": True, "blacklisted_at": now}},
        )
        # modified_count > 0 berarti KITA yang mentrigger transisi blokir —
        # kalau 0, sudah keburu di-set True oleh request lain (race bareng).
        became_blacklisted = result.modified_count > 0

    if became_blacklisted:
        await _refresh_blacklist_cache(force=True)

    return count, became_blacklisted


def _normalize_count(doc: dict) -> dict:
    """Pastikan field `count` SELALU sinkron dengan len(reporters) saat
    dibaca — menutup kemungkinan dokumen lama/basi menampilkan angka yang
    tidak sesuai jumlah pelapor sebenarnya di /cekreport & /cekstickerpack."""
    doc["count"] = len(doc.get("reporters", []))
    return doc


async def get_blacklisted_packs() -> list[dict]:
    """Daftar semua dokumen pack yang sedang blacklisted=True (untuk owner)."""
    docs = await sticker_report_db.find({"blacklisted": True}).to_list()
    return [_normalize_count(d) for d in docs]


async def get_pending_packs() -> list[dict]:
    """Daftar semua dokumen pack yang SUDAH dilaporkan tapi BELUM mencapai
    ambang blokir (blacklisted=False) — untuk owner pantau progres laporan."""
    docs = await sticker_report_db.find({"blacklisted": False}).to_list()
    return [_normalize_count(d) for d in docs]


async def open_sticker_pack(set_name: str) -> bool:
    """
    Buka blokir `set_name` (jika sedang diblokir) & reset hitungan + daftar
    pelapor ke 0/kosong, supaya pack bisa dilaporkan ulang dari awal.
    Return False jika pack tidak ditemukan sama sekali di database.
    """
    doc = await sticker_report_db.find_one({"_id": set_name})
    if not doc:
        return False

    await sticker_report_db.update_one(
        {"_id": set_name},
        {"$set": {
            "blacklisted":    False,
            "blacklisted_at": None,
            "count":          0,
            "reporters":      [],
            "updated_at":     time.time(),
        }},
    )
    await _refresh_blacklist_cache(force=True)
    return True
