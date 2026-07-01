"""
database.py — Multi-Backend Database Engine
─────────────────────────────────────────────
Otomatis memilih backend terbaik yang tersedia:

  Prioritas:
    1. MONGO_URL di .env  → MongoDB (via motor)
    2. SQLITE_PATH di .env → SQLite lokal (default Termux)

  Saat startup, bot mencoba koneksi MongoDB terlebih dahulu.
  Jika gagal (URL tidak ada / error jaringan / auth gagal),
  otomatis fallback ke SQLite tanpa crash.

  Log backend yang aktif muncul di terminal Termux saat start.

  Semua Collection API identik di kedua backend sehingga
  TIDAK ADA file lain yang perlu diubah.
"""

from __future__ import annotations

import os
import json
import time
import uuid
import asyncio
import aiosqlite
from datetime import datetime, timedelta, timezone
from pyrogram.enums import ChatMemberStatus
from dotenv import load_dotenv
from pathlib import Path as _Path

from core import mongo_shard as _shard

# Cari .env relatif ke file ini, bukan CWD — aman dijalankan dari direktori manapun
load_dotenv(dotenv_path=_Path(__file__).parent / ".env", override=False)

# ── CODE_BOT: namespace isolasi database per-bot ─────────────────────────────
# Semua nama collection akan di-prefix dengan CODE_BOT.
# Dua bot dengan CODE_BOT sama → pakai database yang sama (berbagi data).
# Dua bot dengan CODE_BOT beda di MongoDB/SQLite yang sama → koleksi terpisah, tidak campur.
# Jika CODE_BOT kosong → nama collection tidak di-prefix (perilaku lama).
import re as _re
_CODE_BOT_RAW = os.environ.get("CODE_BOT", "").strip()
_CODE_BOT     = _re.sub(r"[^a-zA-Z0-9]", "_", _CODE_BOT_RAW).lower().strip("_") if _CODE_BOT_RAW else ""

def _ns(name: str) -> str:
    """Tambahkan CODE_BOT prefix ke nama collection.
    Contoh: CODE_BOT=mybot → 'nexus_kalimat' jadi 'mybot_nexus_kalimat'
    """
    return f"{_CODE_BOT}_{name}" if _CODE_BOT else name


# ══════════════════════════════════════════════════════════════════════════════
# KONFIGURASI
# ══════════════════════════════════════════════════════════════════════════════

MONGO_URL            = os.environ.get("MONGO_URL", "").strip()

# ── Data directory: selalu di home user, berdasarkan CODE_BOT ─────────────────
# Format: ~/.nexusai/<CODE_BOT>/
# Dengan ini data SELALU ditemukan dari direktori manapun bot dijalankan,
# dan CODE_BOT yang sama selalu mengakses data yang sama.
_BOT_KEY    = _CODE_BOT if _CODE_BOT else "_default"
_DATA_DIR   = _Path.home() / ".nexusai" / _BOT_KEY
_DATA_DIR.mkdir(parents=True, exist_ok=True)

# MONGO_DB_NAME: dari .env jika diset, fallback ke "nexusai_<CODE_BOT>"
# Sehingga dua CODE_BOT berbeda otomatis pakai database MongoDB berbeda
_MONGO_DB_DEFAULT = f"nexusai_{_BOT_KEY}"
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "").strip() or _MONGO_DB_DEFAULT

# SQLITE_PATH: dari .env jika diset (harus path absolut),
# fallback ke ~/.nexusai/<CODE_BOT>/nexus_bot.db — selalu ditemukan
_SQLITE_DEFAULT = str(_DATA_DIR / "nexus_bot.db")
_SQLITE_ENV     = os.environ.get("SQLITE_PATH", "").strip()
SQLITE_PATH     = _SQLITE_ENV if _SQLITE_ENV else _SQLITE_DEFAULT

GLOBAL_EXPIRY        = 15
DEFAULT_LOCAL_EXPIRY = 3600
TZ_WIB               = timezone(timedelta(hours=7))

DEFAULT_CONFIG = {
    "local":            False,
    "global":           True,
    "expiry":           DEFAULT_LOCAL_EXPIRY,
    "bio_check":        False,
    "bio_vip_text":     "",   # teks VIP bio — user dengan teks ini di bio = VIP, bebas dari semua filter
    "anti_mention":            False,
    "mention_batasi_akun":     True,   # batasi mention ke user non-member
    "mention_batasi_channel":  True,   # batasi mention ke channel
    "mention_batasi_grup":     True,   # batasi mention ke grup/supergroup
    "anti_link":        True,  # toggle link detector (URL/tautan aktif dalam pesan)
    "cas":              False,
    "local_spam_limit": 1,    # berapa pesan terakhir yg diingat untuk cek duplikat lokal (1-5)
    "anti_spam_ai":     False, # Nexus AI murni + auto regex aktif/nonaktif per grup (default OFF)
    "vip_title_enabled": False, # Title VIP: tag otomatis (setChatMemberTag) untuk SEMUA member
                             # VIP (manual /vip ATAU bio_vip) di grup ini.
    "vip_title":         "",   # Teks tag (maks 16 UTF-16 code unit, batas Telegram) yang dipasang
                             # ke setiap Member VIP saat vip_title_enabled=True. Kosong = fitur
                             # tidak memasang tag apapun walau enabled=True (sama seperti
                             # auto_title_names kosong di NewsCore).
    "ubot_detect":      True, # Deteksi Ubot: rekam kalimat per-user, tandai
                              # perilaku ubot kalau semua variasi kalimat user
                              # itu sudah terkirim \u22653x tanpa ada kalimat baru.
                              # Rekaman kalimat berjalan terus selama minimal 1
                              # fitur bot ON di grup ini, terlepas status toggle
                              # fitur ini sendiri (lihat core/ubot_detect.py).
}

# ── In-memory cache ────────────────────────────────────────────────────────────
_config_cache: dict[int, tuple[dict, float]] = {}
_admin_cache:  dict[tuple, tuple[bool, float]] = {}
CONFIG_TTL = 10
ADMIN_TTL  = int(os.environ.get("ADMIN_CACHE_TTL", 120))

# ── Panel UI cache (mempercepat tombol DM panel agar tidak query DB tiap klik) ─
_ns_config_cache:   dict[int, tuple[dict, float]]  = {}  # ns_get_config
_regex_count_cache: dict[int, tuple[int,  float]]  = {}  # count regex per grup
_free_count_cache:  dict[int, tuple[int,  float]]  = {}  # count VIP per grup
_admin_groups_cache: dict[int, tuple[list, float]] = {}  # get_my_admin_groups per user
NS_CONFIG_TTL    = 30   # detik — ns_config jarang berubah
COUNT_TTL        = 30   # detik — count regex/VIP
ADMIN_GROUPS_TTL = 120  # detik — daftar grup admin (2 menit)

# ── Nexus AI panel cache ───────────────────────────────────────────────────────
_nexus_kalimat_count_cache: tuple[tuple[int,int], float] | None = None
_nexus_regex_count_cache:   tuple[int, float]            | None = None
_nexus_wl_count_cache:      tuple[int, float]            | None = None
_nexus_owner_regex_count_cache: tuple[int, float]        | None = None
_nexus_grup_cache:          tuple[list, float]           | None = None
NEXUS_COUNT_TTL = 30   # detik

# ── Delete queue ───────────────────────────────────────────────────────────────
delete_queue: asyncio.Queue = asyncio.Queue()

# ── Panel write queue ──────────────────────────────────────────────────────────
# Tujuan: tombol panel DM (toggle, +/-, dsb) terasa "ringan" — UI berubah instan
# karena cache di-update duluan (optimistic), sedangkan penulisan ke DB yang
# sesungguhnya diantrikan dan dieksekusi belakangan oleh satu worker tunggal,
# dengan jeda antar-item supaya tidak membebani DB/API saat banyak grup/klik
# bersamaan.
#
# Jika penulisan GAGAL PERMANEN (sudah di-retry beberapa kali, tetap gagal):
#   1. Cache untuk chat_id tersebut di-invalidate (paksa baca ulang dari DB
#      di klik berikutnya — otomatis dapat nilai asli, bukan nilai optimistic
#      yang ternyata tidak pernah tersimpan).
#   2. Jika item membawa info pesan panel asal (dm_chat_id + dm_msg_id),
#      panggil _panel_rollback_callback (didaftarkan oleh handlers_dm.py saat
#      startup) untuk mengoreksi tampilan panel itu + beri tahu admin.
# Jika sukses → tidak ada apa-apa (silent), karena UI sudah benar dari awal.
panel_write_queue: asyncio.Queue = asyncio.Queue()
PANEL_WRITE_DELAY   = 0.3   # detik — jeda antar penulisan ke DB
PANEL_WRITE_RETRIES = 3     # percobaan ulang sebelum dianggap gagal permanen

# ── Shared FloodWait State — koordinasi lintas worker ─────────────────────────
# Saat salah satu worker kena FloodWait dari Telegram, worker lain harus
# berhenti juga agar tidak memperparah flood. Setiap worker memanggil
# wait_global_flood_backoff() sebelum API call berat, dan memanggil
# set_global_flood_backoff(seconds) saat kena FloodWait.
#
# Ini TIDAK menggantikan FloodWait handling lokal masing-masing worker —
# melainkan lapisan koordinasi ANTAR worker: jika delete_worker kena FloodWait
# 10 detik, log_flush_worker dan moderation_worker tidak ikut tembak API
# selama window itu.
import time as _time_module

_global_flood_until: float = 0.0   # monotonic timestamp saat backoff selesai


def set_global_flood_backoff(seconds: float) -> None:
    """Catat bahwa Telegram baru kirim FloodWait. Semua worker akan mundur."""
    global _global_flood_until
    deadline = _time_module.monotonic() + seconds
    if deadline > _global_flood_until:
        _global_flood_until = deadline


async def wait_global_flood_backoff() -> None:
    """
    Tunggu jika ada global flood backoff aktif.
    Dipanggil oleh tiap worker sebelum API call berat (send, delete, restrict).
    Jika backoff sudah lewat, langsung return tanpa delay.
    """
    remaining = _global_flood_until - _time_module.monotonic()
    if remaining > 0:
        await asyncio.sleep(remaining)

# Diisi oleh plugins/ui/handlers_dm.py via register_panel_rollback_callback().
# Signature: async def callback(client, kind, chat_id, key, dm_chat_id, dm_msg_id) -> None
_panel_rollback_callback = None


def register_panel_rollback_callback(fn) -> None:
    """Daftarkan fungsi yang dipanggil saat penulisan panel gagal permanen."""
    global _panel_rollback_callback
    _panel_rollback_callback = fn

# ── Handled messages tracker ──────────────────────────────────────────────────
_handled_msgs: dict[tuple[int, int], float] = {}
_HANDLED_TTL = 30.0

# ── Backend state ─────────────────────────────────────────────────────────────
_BACKEND: str = "sqlite"   # "mongo" | "sqlite"
_mongo_db = None           # motor database instance (jika aktif)
_sqlite_conn: aiosqlite.Connection | None = None


# ══════════════════════════════════════════════════════════════════════════════
# BACKEND DETECTION — dipanggil sekali di setup_db()
# ══════════════════════════════════════════════════════════════════════════════

async def _try_mongo(url: str, db_name: str):
    """
    Coba koneksi ke MongoDB. Return motor database object jika berhasil,
    None jika gagal. Timeout 5 detik agar tidak hang di Termux.
    """
    try:
        import dns.resolver
        dns.resolver.default_resolver = dns.resolver.Resolver(configure=False)
        dns.resolver.default_resolver.nameservers = ['1.1.1.1', '1.0.0.1']
        import motor.motor_asyncio as motor  # type: ignore
        client = motor.AsyncIOMotorClient(
            url,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=5000,
        )
        # Ping untuk memastikan koneksi benar-benar berhasil
        await client.admin.command("ping")
        return client[db_name]
    except ImportError:
        print("[DB] motor tidak terinstall — skip MongoDB")
        return None
    except Exception as e:
        print(f"[DB] MongoDB gagal: {e}")
        return None


async def _init_backend():
    """
    Tentukan backend aktif dan inisialisasi koneksi.
    Urutan: MongoDB (multi-shard jika MONGO_URL_2, MONGO_URL_3, ... diisi) → SQLite.

    MULTI-SHARD:
      Setiap MONGO_URL_n di .env dianggap 1 cluster Mongo independen.
      Semua cluster dicoba konek secara paralel saat startup. Cluster yang
      gagal konek TIDAK menggagalkan keseluruhan startup — collection yang
      ter-assign ke shard itu (lihat core/mongo_shard.py) otomatis fallback
      ke SQLite lokal khusus shard tersebut (_get_sqlite_shard), sementara
      shard lain yang sehat tetap berjalan normal di MongoDB.

      Jika hanya MONGO_URL terisi (tidak ada MONGO_URL_2 dst) → perilaku
      identik dengan versi sebelumnya: 1 cluster, tidak ada perubahan.
    """
    global _BACKEND, _mongo_db, _sqlite_conn

    urls = _shard.MONGO_URLS
    if urls:
        print(f"[DB] 🔍 Mencoba koneksi {len(urls)} shard MongoDB...")
        results = await asyncio.gather(*[_try_mongo(u, MONGO_DB_NAME) for u in urls])
        any_ok = False
        for idx, mongo in enumerate(results):
            _shard.set_shard_db(idx, mongo)
            if mongo is not None:
                any_ok = True
                tag = f"shard{idx}" if len(urls) > 1 else "MongoDB"
                print(f"[DB] ✅ {tag} aktif (db={MONGO_DB_NAME})")
            else:
                print(f"[DB] ⚠️  shard{idx} gagal konek → fallback SQLite khusus shard ini")
        if any_ok:
            _BACKEND  = "mongo"
            _mongo_db = _shard.get_shard_db(0)   # compat lama: kode yang akses _mongo_db langsung tetap dapat shard utama
            print(f"[DB] ✅ BACKEND AKTIF: MongoDB  ({_shard.shard_summary()})")
            return
        print("[DB] ⚠️  Semua shard MongoDB gagal → fallback total ke SQLite")
    else:
        print("[DB] ℹ️  MONGO_URL tidak ditemukan di .env → pakai SQLite")

    # ── Fallback SQLite (penuh, tidak ada shard Mongo yang hidup) ────────────
    _BACKEND = "sqlite"
    _sqlite_conn = await aiosqlite.connect(SQLITE_PATH, check_same_thread=False)
    await _sqlite_conn.execute("PRAGMA journal_mode=WAL")
    await _sqlite_conn.execute("PRAGMA synchronous=NORMAL")
    _sqlite_conn.row_factory = aiosqlite.Row
    abs_path = os.path.abspath(SQLITE_PATH)
    print(f"[DB] ✅ BACKEND AKTIF: SQLite     (file={abs_path})")


def get_active_backend() -> str:
    """Kembalikan nama backend aktif: 'mongo' atau 'sqlite'."""
    return _BACKEND


# ══════════════════════════════════════════════════════════════════════════════
# JSON ENCODER — handle datetime & bytes (untuk SQLite backend)
# ══════════════════════════════════════════════════════════════════════════════

class _Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return {"__dt__": obj.isoformat()}
        if isinstance(obj, bytes):
            return {"__bytes__": obj.hex()}
        return super().default(obj)


def _object_hook(obj: dict):
    if "__dt__" in obj:
        try:
            return datetime.fromisoformat(obj["__dt__"])
        except Exception:
            return obj
    if "__bytes__" in obj:
        try:
            return bytes.fromhex(obj["__bytes__"])
        except Exception:
            return obj
    return obj


def _dumps(obj) -> str:
    return json.dumps(obj, cls=_Encoder, ensure_ascii=False)


def _loads(s: str) -> dict:
    return json.loads(s, object_hook=_object_hook)


# ══════════════════════════════════════════════════════════════════════════════
# SQLITE HELPERS (internal)
# ══════════════════════════════════════════════════════════════════════════════

async def _get_sqlite() -> aiosqlite.Connection:
    global _sqlite_conn
    if _sqlite_conn is None:
        _sqlite_conn = await aiosqlite.connect(SQLITE_PATH, check_same_thread=False)
        await _sqlite_conn.execute("PRAGMA journal_mode=WAL")
        await _sqlite_conn.execute("PRAGMA synchronous=NORMAL")
        _sqlite_conn.row_factory = aiosqlite.Row
    return _sqlite_conn


# ── SQLite per-shard (fallback granular saat 1 cluster Mongo down) ───────────
# Dipakai HANYA oleh collection yang masuk SHARDED_COLLECTIONS saat shard
# Mongo yang dituju sedang tidak sehat. File terpisah per shard index agar
# tidak rebutan lock dengan _sqlite_conn (shard 0 / fallback total).
_shard_sqlite_conns: dict[int, aiosqlite.Connection] = {}


async def _get_sqlite_for_shard(idx: int) -> aiosqlite.Connection:
    if idx == 0:
        # Shard 0 berbagi file yang sama dengan fallback SQLite lama —
        # tidak perlu file baru, menjaga kompatibilitas data lama.
        return await _get_sqlite()
    global _shard_sqlite_conns
    conn = _shard_sqlite_conns.get(idx)
    if conn is None:
        path = SQLITE_PATH.replace(".db", f".shard{idx}.db") if SQLITE_PATH.endswith(".db") else f"{SQLITE_PATH}.shard{idx}"
        conn = await aiosqlite.connect(path, check_same_thread=False)
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = aiosqlite.Row
        _shard_sqlite_conns[idx] = conn
    return conn


def _tbl(name: str) -> str:
    return "col_" + name.replace("-", "_").replace(" ", "_")


async def _ensure_table(conn: aiosqlite.Connection, name: str):
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {_tbl(name)} (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id TEXT    UNIQUE,
            data   TEXT    NOT NULL
        )
    """)
    await conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# QUERY MATCHING — MongoDB-style (untuk SQLite backend)
# ══════════════════════════════════════════════════════════════════════════════

def _match(doc: dict, query: dict) -> bool:
    if not query:
        return True
    for key, val in query.items():
        doc_val = doc.get(key)
        if isinstance(val, dict):
            for op, op_val in val.items():
                if op == "$exists":
                    if bool(op_val) != (key in doc):
                        return False
                elif op == "$ne":
                    if doc_val == op_val:
                        return False
                elif op == "$gt":
                    if not (doc_val is not None and doc_val > op_val):
                        return False
                elif op == "$lt":
                    if not (doc_val is not None and doc_val < op_val):
                        return False
                elif op == "$in":
                    if doc_val not in op_val:
                        return False
        else:
            if doc_val != val:
                return False
    return True


def _apply_update(doc: dict, update: dict, is_insert: bool = False) -> dict:
    result = dict(doc)
    if "$set" in update:
        result.update(update["$set"])
    if "$setOnInsert" in update and is_insert:
        result.update(update["$setOnInsert"])
    if "$inc" in update:
        for k, v in update["$inc"].items():
            result[k] = (result.get(k) or 0) + v
    if "$unset" in update:
        for k in update["$unset"]:
            result.pop(k, None)
    if "$push" in update:
        for k, v in update["$push"].items():
            if k not in result or not isinstance(result[k], list):
                result[k] = []
            result[k].append(v)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# RESULT OBJECTS
# ══════════════════════════════════════════════════════════════════════════════

class DeleteResult:
    def __init__(self, count: int = 0):
        self.deleted_count = count


class UpdateResult:
    def __init__(self, matched: int = 0, modified: int = 0, upserted_id=None):
        self.matched_count  = matched
        self.modified_count = modified
        self.upserted_id    = upserted_id


# ══════════════════════════════════════════════════════════════════════════════
# ASYNC CURSOR — mimic motor cursor API
# ══════════════════════════════════════════════════════════════════════════════

class AsyncCursor:
    """
    Unified cursor untuk SQLite dan MongoDB.
    SQLite: load semua data lalu filter in-memory.
    MongoDB: delegasi ke motor cursor dengan sort/skip/limit native.
    """

    def __init__(self, col_name: str, query: dict):
        self._col      = col_name
        self._query    = query
        self._sort_key: str | None = None
        self._sort_dir: int        = 1
        self._skip_n:   int        = 0
        self._limit_n:  int | None = None
        self._docs:     list[dict] | None = None
        self._pos:      int        = 0
        # MongoDB motor cursor (lazy)
        self._mongo_cur = None

    def sort(self, key: str, direction: int = 1) -> "AsyncCursor":
        self._sort_key = key
        self._sort_dir = direction
        return self

    def skip(self, n: int) -> "AsyncCursor":
        self._skip_n = n
        return self

    def limit(self, n: int) -> "AsyncCursor":
        self._limit_n = n
        return self

    # ── SQLite path ───────────────────────────────────────────────────────────
    async def _load_sqlite(self, shard_idx: int = 0):
        conn = await _get_sqlite_for_shard(shard_idx) if _BACKEND == "mongo" else await _get_sqlite()
        tbl  = _tbl(self._col)
        await _ensure_table(conn, self._col)
        async with conn.execute(f"SELECT id, data FROM {tbl} ORDER BY id") as cur:
            rows = await cur.fetchall()
        docs = []
        for row in rows:
            try:
                d = _loads(row["data"])
                if "_id" not in d:
                    d["_id"] = str(row["id"])
                if _match(d, self._query):
                    docs.append(d)
            except Exception:
                pass
        return docs

    def _apply_sort_skip_limit(self, docs: list[dict]) -> list[dict]:
        if self._sort_key:
            docs = sorted(
                docs,
                key=lambda d: (d.get(self._sort_key) or ""),
                reverse=(self._sort_dir == -1),
            )
        docs = docs[self._skip_n:]
        if self._limit_n is not None:
            docs = docs[:self._limit_n]
        return docs

    # ── MongoDB path ──────────────────────────────────────────────────────────
    async def _load_mongo(self):
        bare = _bare_collection_name(self._col)
        cid  = _shard.extract_chat_id(self._query)

        if bare in _shard.SHARDED_COLLECTIONS and _shard.SHARD_COUNT > 1 and cid is None:
            # Query tanpa chat_id spesifik → harus baca SEMUA shard dan gabung
            # (sort/skip/limit diterapkan in-memory setelah gabung, karena
            # makna "skip 10 limit 20" global lintas shard tidak bisa dipush
            # secara native ke masing-masing shard tanpa hasil salah).
            all_docs: list[dict] = []
            for idx in range(_shard.SHARD_COUNT):
                col = _mongo_col_for(self._col, idx)
                if col is not None:
                    try:
                        async for doc in col.find(self._query):
                            doc["_id"] = str(doc["_id"])
                            all_docs.append(doc)
                        continue
                    except Exception as e:
                        print(f"[DB:mongo] find error {self._col} (shard{idx}): {e}")
                        _shard.mark_shard_down(idx)
                all_docs.extend(await self._load_sqlite(idx))
            self._docs = self._apply_sort_skip_limit(all_docs)
            return

        shard_idx = _resolve_shard_idx(self._col, self._query)
        col = _mongo_col_for(self._col, shard_idx)
        if col is not None:
            try:
                cur = col.find(self._query)
                if self._sort_key:
                    cur = cur.sort(self._sort_key, self._sort_dir)
                if self._skip_n:
                    cur = cur.skip(self._skip_n)
                if self._limit_n is not None:
                    cur = cur.limit(self._limit_n)
                docs = []
                async for doc in cur:
                    doc["_id"] = str(doc["_id"])
                    docs.append(doc)
                self._docs = docs
                return
            except Exception as e:
                print(f"[DB:mongo] find error {self._col} (shard{shard_idx}): {e}")
                _shard.mark_shard_down(shard_idx)
        # fallback sqlite shard ini
        docs = await self._load_sqlite(shard_idx)
        self._docs = self._apply_sort_skip_limit(docs)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._docs is None:
            if _BACKEND == "mongo":
                await self._load_mongo()
            else:
                self._docs = self._apply_sort_skip_limit(await self._load_sqlite())
        if self._pos >= len(self._docs):
            raise StopAsyncIteration
        doc       = self._docs[self._pos]
        self._pos += 1
        return doc

    async def to_list(self, length: int | None = None) -> list[dict]:
        if self._docs is None:
            if _BACKEND == "mongo":
                await self._load_mongo()
            else:
                self._docs = self._apply_sort_skip_limit(await self._load_sqlite())
        if length is not None:
            return self._docs[:length]
        return list(self._docs)


# ══════════════════════════════════════════════════════════════════════════════
# SHARD RESOLUTION — pilih cluster Mongo / SQLite yang tepat untuk operasi
# ══════════════════════════════════════════════════════════════════════════════
#
# Hanya collection di _shard.SHARDED_COLLECTIONS (lihat core/mongo_shard.py,
# nama TANPA prefix _ns) yang di-route berdasarkan chat_id. Sisanya selalu
# memakai shard 0 (cluster utama) — jumlahnya kecil dan tidak perlu dipecah.
#
# bare_name: nama collection SEBELUM di-prefix _ns(), karena SHARDED_COLLECTIONS
# didefinisikan dengan nama generik (mis. "bio_profiles"), bukan
# "mybot_bio_profiles". Collection.name yang disimpan di __init__ SUDAH
# di-_ns()-kan oleh DB.__getitem__, jadi kita strip prefix CODE_BOT dulu
# untuk pencocokan, dengan fallback ke endswith() agar tetap aman.

def _bare_collection_name(ns_name: str) -> str:
    if _CODE_BOT and ns_name.startswith(_CODE_BOT + "_"):
        return ns_name[len(_CODE_BOT) + 1:]
    return ns_name


def _resolve_shard_idx(ns_name: str, *dicts: dict) -> int:
    """Tentukan shard index untuk operasi pada collection ns_name, berdasarkan
    chat_id yang ditemukan di salah satu dict (query dan/atau doc/update)."""
    bare = _bare_collection_name(ns_name)
    if bare not in _shard.SHARDED_COLLECTIONS or _shard.SHARD_COUNT <= 1:
        return 0
    for d in dicts:
        if not isinstance(d, dict):
            continue
        # cek langsung, lalu cek di dalam $set (update_one biasa berbentuk {"$set": {...}})
        cid = _shard.extract_chat_id(d)
        if cid is None and "$set" in d:
            cid = _shard.extract_chat_id(d["$set"])
        if cid is None and "$setOnInsert" in d:
            cid = _shard.extract_chat_id(d["$setOnInsert"])
        if cid is not None:
            return _shard.shard_index_for_chat(cid)
    return 0  # tidak ada chat_id ditemukan → aman ke shard utama


def _mongo_col_for(ns_name: str, shard_idx: int):
    """Return motor collection object di shard tertentu, atau None jika
    shard itu sedang tidak sehat (caller harus fallback SQLite shard ini)."""
    shard_db = _shard.get_shard_db(shard_idx)
    if shard_db is None or not _shard.is_shard_healthy(shard_idx):
        return None
    return shard_db[ns_name]


# ══════════════════════════════════════════════════════════════════════════════
# COLLECTION — unified API untuk MongoDB dan SQLite
# ══════════════════════════════════════════════════════════════════════════════

class Collection:
    def __init__(self, name: str):
        self.name = name

    # ── find_one ──────────────────────────────────────────────────────────────

    async def find_one(self, query: dict = {}) -> dict | None:
        if _BACKEND == "mongo":
            shard_idx = _resolve_shard_idx(self.name, query)
            col = _mongo_col_for(self.name, shard_idx)
            if col is not None:
                try:
                    doc = await col.find_one(query)
                    if doc:
                        doc["_id"] = str(doc["_id"])
                    return doc
                except Exception as e:
                    print(f"[DB:mongo] find_one error {self.name} (shard{shard_idx}): {e}")
                    _shard.mark_shard_down(shard_idx)
                    # lanjut ke fallback SQLite shard ini di bawah
            # Shard down / error → fallback SQLite khusus shard ini
            conn = await _get_sqlite_for_shard(shard_idx)
        else:
            conn = await _get_sqlite()
        tbl  = _tbl(self.name)
        await _ensure_table(conn, self.name)
        async with conn.execute(f"SELECT id, data FROM {tbl} ORDER BY id") as cur:
            rows = await cur.fetchall()
        for row in rows:
            try:
                d = _loads(row["data"])
                if "_id" not in d:
                    d["_id"] = str(row["id"])
                if _match(d, query):
                    return d
            except Exception:
                pass
        return None

    # ── find ──────────────────────────────────────────────────────────────────

    def find(self, query: dict = {}) -> AsyncCursor:
        return AsyncCursor(self.name, query)

    # ── update_one ────────────────────────────────────────────────────────────

    async def update_one(
        self, filter_q: dict, update: dict, upsert: bool = False
    ) -> UpdateResult:
        if _BACKEND == "mongo":
            shard_idx = _resolve_shard_idx(self.name, filter_q, update)
            col = _mongo_col_for(self.name, shard_idx)
            if col is not None:
                try:
                    r = await col.update_one(filter_q, update, upsert=upsert)
                    return UpdateResult(r.matched_count, r.modified_count, str(r.upserted_id) if r.upserted_id else None)
                except Exception as e:
                    print(f"[DB:mongo] update_one error {self.name} (shard{shard_idx}): {e}")
                    _shard.mark_shard_down(shard_idx)
            conn = await _get_sqlite_for_shard(shard_idx)
        else:
            conn = await _get_sqlite()
        tbl  = _tbl(self.name)
        await _ensure_table(conn, self.name)
        found_id, found_doc = None, None
        async with conn.execute(f"SELECT id, data FROM {tbl} ORDER BY id") as cur:
            rows = await cur.fetchall()
        for row in rows:
            try:
                d = _loads(row["data"])
                if "_id" not in d:
                    d["_id"] = str(row["id"])
                if _match(d, filter_q):
                    found_id  = row["id"]
                    found_doc = d
                    break
            except Exception:
                pass
        if found_doc is not None:
            new_doc = _apply_update(found_doc, update, is_insert=False)
            await conn.execute(f"UPDATE {tbl} SET data=? WHERE id=?", (_dumps(new_doc), found_id))
            await conn.commit()
            return UpdateResult(matched=1, modified=1)
        if upsert:
            new_doc = {}
            new_doc.update(filter_q)
            new_doc = _apply_update(new_doc, update, is_insert=True)
            doc_id  = str(new_doc.get("_id") or uuid.uuid4().hex)
            new_doc["_id"] = doc_id
            await conn.execute(
                f"INSERT OR REPLACE INTO {tbl} (doc_id, data) VALUES (?, ?)",
                (doc_id, _dumps(new_doc))
            )
            await conn.commit()
            return UpdateResult(matched=0, modified=0, upserted_id=doc_id)
        return UpdateResult()

    # ── update_many ───────────────────────────────────────────────────────────

    async def update_many(self, filter_q: dict, update: dict) -> UpdateResult:
        if _BACKEND == "mongo":
            bare = _bare_collection_name(self.name)
            cid  = _shard.extract_chat_id(filter_q)
            if bare in _shard.SHARDED_COLLECTIONS and _shard.SHARD_COUNT > 1 and cid is None:
                # Filter tidak menyebut chat_id spesifik (mis. cleanup TTL massal)
                # → harus menjangkau SEMUA shard, bukan hanya shard 0.
                total_matched, total_modified = 0, 0
                for idx in range(_shard.SHARD_COUNT):
                    col = _mongo_col_for(self.name, idx)
                    if col is None:
                        continue
                    try:
                        r = await col.update_many(filter_q, update)
                        total_matched  += r.matched_count
                        total_modified += r.modified_count
                    except Exception as e:
                        print(f"[DB:mongo] update_many error {self.name} (shard{idx}): {e}")
                        _shard.mark_shard_down(idx)
                return UpdateResult(total_matched, total_modified)
            shard_idx = _resolve_shard_idx(self.name, filter_q, update)
            col = _mongo_col_for(self.name, shard_idx)
            if col is not None:
                try:
                    r = await col.update_many(filter_q, update)
                    return UpdateResult(r.matched_count, r.modified_count)
                except Exception as e:
                    print(f"[DB:mongo] update_many error {self.name} (shard{shard_idx}): {e}")
                    _shard.mark_shard_down(shard_idx)
            conn = await _get_sqlite_for_shard(shard_idx)
        else:
            conn = await _get_sqlite()
        # SQLite
        tbl  = _tbl(self.name)
        await _ensure_table(conn, self.name)
        async with conn.execute(f"SELECT id, data FROM {tbl}") as cur:
            rows = await cur.fetchall()
        modified = 0
        for row in rows:
            try:
                d = _loads(row["data"])
                if "_id" not in d:
                    d["_id"] = str(row["id"])
                if not filter_q or _match(d, filter_q):
                    new_doc = _apply_update(d, update, is_insert=False)
                    await conn.execute(f"UPDATE {tbl} SET data=? WHERE id=?", (_dumps(new_doc), row["id"]))
                    modified += 1
            except Exception:
                pass
        if modified:
            await conn.commit()
        return UpdateResult(matched=modified, modified=modified)

    # ── insert_one ────────────────────────────────────────────────────────────

    async def insert_one(self, doc: dict) -> UpdateResult:
        if _BACKEND == "mongo":
            shard_idx = _resolve_shard_idx(self.name, doc)
            col = _mongo_col_for(self.name, shard_idx)
            if col is not None:
                try:
                    d = dict(doc)
                    d.pop("_id", None)
                    r = await col.insert_one(d)
                    return UpdateResult(upserted_id=str(r.inserted_id))
                except Exception as e:
                    print(f"[DB:mongo] insert_one error {self.name} (shard{shard_idx}): {e}")
                    _shard.mark_shard_down(shard_idx)
            conn = await _get_sqlite_for_shard(shard_idx)
        else:
            conn = await _get_sqlite()
        tbl    = _tbl(self.name)
        await _ensure_table(conn, self.name)
        doc_id = str(doc.get("_id") or uuid.uuid4().hex)
        d      = dict(doc)
        d["_id"] = doc_id
        try:
            await conn.execute(
                f"INSERT OR IGNORE INTO {tbl} (doc_id, data) VALUES (?, ?)",
                (doc_id, _dumps(d))
            )
            await conn.commit()
        except Exception:
            pass
        return UpdateResult(upserted_id=doc_id)

    # ── delete_one ────────────────────────────────────────────────────────────

    async def delete_one(self, query: dict) -> DeleteResult:
        if _BACKEND == "mongo":
            shard_idx = _resolve_shard_idx(self.name, query)
            col = _mongo_col_for(self.name, shard_idx)
            if col is not None:
                try:
                    r = await col.delete_one(query)
                    return DeleteResult(r.deleted_count)
                except Exception as e:
                    print(f"[DB:mongo] delete_one error {self.name} (shard{shard_idx}): {e}")
                    _shard.mark_shard_down(shard_idx)
            conn = await _get_sqlite_for_shard(shard_idx)
        else:
            conn = await _get_sqlite()
        tbl  = _tbl(self.name)
        await _ensure_table(conn, self.name)
        async with conn.execute(f"SELECT id, data FROM {tbl} ORDER BY id") as cur:
            rows = await cur.fetchall()
        for row in rows:
            try:
                d = _loads(row["data"])
                if "_id" not in d:
                    d["_id"] = str(row["id"])
                if _match(d, query):
                    await conn.execute(f"DELETE FROM {tbl} WHERE id=?", (row["id"],))
                    await conn.commit()
                    return DeleteResult(1)
            except Exception:
                pass
        return DeleteResult(0)

    # ── delete_many ───────────────────────────────────────────────────────────

    async def delete_many(self, query: dict = {}) -> DeleteResult:
        if _BACKEND == "mongo":
            bare = _bare_collection_name(self.name)
            cid  = _shard.extract_chat_id(query)
            if bare in _shard.SHARDED_COLLECTIONS and _shard.SHARD_COUNT > 1 and cid is None:
                total = 0
                for idx in range(_shard.SHARD_COUNT):
                    col = _mongo_col_for(self.name, idx)
                    if col is None:
                        continue
                    try:
                        r = await col.delete_many(query)
                        total += r.deleted_count
                    except Exception as e:
                        print(f"[DB:mongo] delete_many error {self.name} (shard{idx}): {e}")
                        _shard.mark_shard_down(idx)
                return DeleteResult(total)
            shard_idx = _resolve_shard_idx(self.name, query)
            col = _mongo_col_for(self.name, shard_idx)
            if col is not None:
                try:
                    r = await col.delete_many(query)
                    return DeleteResult(r.deleted_count)
                except Exception as e:
                    print(f"[DB:mongo] delete_many error {self.name} (shard{shard_idx}): {e}")
                    _shard.mark_shard_down(shard_idx)
            conn = await _get_sqlite_for_shard(shard_idx)
        else:
            conn = await _get_sqlite()
        tbl  = _tbl(self.name)
        await _ensure_table(conn, self.name)
        async with conn.execute(f"SELECT id, data FROM {tbl}") as cur:
            rows = await cur.fetchall()
        to_del = []
        for row in rows:
            try:
                d = _loads(row["data"])
                if "_id" not in d:
                    d["_id"] = str(row["id"])
                if not query or _match(d, query):
                    to_del.append(row["id"])
            except Exception:
                to_del.append(row["id"])
        for rid in to_del:
            await conn.execute(f"DELETE FROM {tbl} WHERE id=?", (rid,))
        if to_del:
            await conn.commit()
        return DeleteResult(len(to_del))

    # ── insert_many ───────────────────────────────────────────────────────────

    async def insert_many(self, docs: list[dict]) -> None:
        if not docs:
            return
        if _BACKEND == "mongo":
            bare = _bare_collection_name(self.name)
            if bare in _shard.SHARDED_COLLECTIONS and _shard.SHARD_COUNT > 1:
                # Dokumen bisa milik chat_id berbeda-beda → kelompokkan per shard
                groups: dict[int, list[dict]] = {}
                for d in docs:
                    idx = _resolve_shard_idx(self.name, d)
                    groups.setdefault(idx, []).append(d)
                for idx, group_docs in groups.items():
                    col = _mongo_col_for(self.name, idx)
                    clean = [{k: v for k, v in d.items() if k != "_id"} for d in group_docs]
                    if col is not None:
                        try:
                            await col.insert_many(clean, ordered=False)
                            continue
                        except Exception as e:
                            print(f"[DB:mongo] insert_many error {self.name} (shard{idx}): {e}")
                            _shard.mark_shard_down(idx)
                    # fallback sqlite shard ini untuk grup dokumen ini
                    conn = await _get_sqlite_for_shard(idx)
                    tbl  = _tbl(self.name)
                    await _ensure_table(conn, self.name)
                    for doc in group_docs:
                        doc_id = str(doc.get("_id") or uuid.uuid4().hex)
                        dd = dict(doc)
                        dd["_id"] = doc_id
                        try:
                            await conn.execute(
                                f"INSERT OR IGNORE INTO {tbl} (doc_id, data) VALUES (?, ?)",
                                (doc_id, _dumps(dd))
                            )
                        except Exception:
                            pass
                    await conn.commit()
                return
            col = _mongo_col_for(self.name, 0)
            if col is not None:
                try:
                    clean = [{k: v for k, v in d.items() if k != "_id"} for d in docs]
                    await col.insert_many(clean, ordered=False)
                    return
                except Exception as e:
                    print(f"[DB:mongo] insert_many error {self.name}: {e}")
                    _shard.mark_shard_down(0)
            conn = await _get_sqlite_for_shard(0)
        else:
            conn = await _get_sqlite()
        tbl  = _tbl(self.name)
        await _ensure_table(conn, self.name)
        for doc in docs:
            doc_id = str(doc.get("_id") or uuid.uuid4().hex)
            d      = dict(doc)
            d["_id"] = doc_id
            try:
                await conn.execute(
                    f"INSERT OR IGNORE INTO {tbl} (doc_id, data) VALUES (?, ?)",
                    (doc_id, _dumps(d))
                )
            except Exception:
                pass
        await conn.commit()

    # ── bulk_write ────────────────────────────────────────────────────────────
    # NOTE: hanya mendukung pymongo.UpdateOne (cukup untuk kebutuhan saat ini —
    # ns_flush_score_buffer). Operasi lain (InsertOne/DeleteOne/dst) bisa
    # ditambahkan kalau ada pemanggil baru yang butuh.

    async def bulk_write(self, ops: list, ordered: bool = False) -> "UpdateResult":
        """
        Versi unified dari motor `collection.bulk_write()` — shard-aware untuk
        MongoDB (operasi dikelompokkan per shard berdasarkan chat_id di
        filter masing-masing UpdateOne, lalu dieksekusi 1x bulk_write per
        shard — TETAP 1 round-trip per shard, bukan 1 round-trip per dokumen),
        dan fallback ke update_one satu-satu untuk SQLite (tidak ada operasi
        bulk asli di SQLite, tapi volume kasus pakai ini kecil).

        Tanpa method ini, caller yang mengandalkan `collection._col.bulk_write()`
        langsung (mengasumsikan `Collection` adalah motor collection asli)
        akan selalu gagal dengan AttributeError, lalu (kalau caller punya
        fallback naive) jatuh balik ke N kali update_one per flush — yang
        justru meniadakan tujuan batching ini sama sekali.
        """
        from pymongo import UpdateOne as _UpdateOne

        def _op_parts(op):
            """Ambil (filter, update_doc, upsert) dari UpdateOne secara aman.
            Pymongo menyimpan ini sebagai atribut privat (_filter/_doc/_upsert,
            stabil sejak versi 3.x s/d 4.x yang dipakai project ini — lihat
            requirements.txt), tapi tetap pakai getattr dengan default aman
            agar tidak crash kalau suatu saat nama atributnya berubah."""
            f = getattr(op, "_filter", None) or {}
            d = getattr(op, "_doc", None) or {}
            u = bool(getattr(op, "_upsert", False))
            return f, d, u

        total_matched, total_modified, total_upserted = 0, 0, 0

        if _BACKEND == "mongo":
            bare = _bare_collection_name(self.name)
            sharded = bare in _shard.SHARDED_COLLECTIONS and _shard.SHARD_COUNT > 1

            groups: dict[int, list] = {}
            for op in ops:
                if not isinstance(op, _UpdateOne):
                    continue  # tipe lain belum didukung — lihat catatan di atas
                f, d, _u = _op_parts(op)
                shard_idx = _resolve_shard_idx(self.name, f, d) if sharded else 0
                groups.setdefault(shard_idx, []).append(op)

            for shard_idx, shard_ops in groups.items():
                col = _mongo_col_for(self.name, shard_idx)
                if col is not None:
                    try:
                        r = await col.bulk_write(shard_ops, ordered=ordered)
                        total_matched  += r.matched_count
                        total_modified += r.modified_count
                        total_upserted += len(r.upserted_ids or {})
                        continue
                    except Exception as e:
                        print(f"[DB:mongo] bulk_write error {self.name} (shard{shard_idx}): {e}")
                        _shard.mark_shard_down(shard_idx)
                # Fallback per-op kalau shard ini down — tetap lebih baik
                # daripada gagal total untuk seluruh batch.
                for op in shard_ops:
                    f, d, u = _op_parts(op)
                    try:
                        r = await self.update_one(f, d, upsert=u)
                        total_matched  += r.matched_count
                        total_modified += r.modified_count
                    except Exception as e:
                        print(f"[DB] bulk_write fallback op error {self.name}: {e}")
            return UpdateResult(total_matched, total_modified)

        # SQLite: tidak ada operasi bulk asli — terapkan satu-satu lewat
        # update_one (sudah konsisten dengan jalur SQLite collection lain).
        for op in ops:
            if not isinstance(op, _UpdateOne):
                continue
            f, d, u = _op_parts(op)
            try:
                r = await self.update_one(f, d, upsert=u)
                total_matched  += r.matched_count
                total_modified += r.modified_count
            except Exception as e:
                print(f"[DB] bulk_write sqlite op error {self.name}: {e}")
        return UpdateResult(total_matched, total_modified)

    # ── count_documents ───────────────────────────────────────────────────────

    async def count_documents(self, query: dict = {}) -> int:
        if _BACKEND == "mongo":
            bare = _bare_collection_name(self.name)
            cid  = _shard.extract_chat_id(query)
            if bare in _shard.SHARDED_COLLECTIONS and _shard.SHARD_COUNT > 1 and cid is None:
                total = 0
                for idx in range(_shard.SHARD_COUNT):
                    col = _mongo_col_for(self.name, idx)
                    if col is None:
                        continue
                    try:
                        if query:
                            total += await col.count_documents(query)
                        else:
                            total += await col.estimated_document_count()
                    except Exception as e:
                        print(f"[DB:mongo] count_documents error {self.name} (shard{idx}): {e}")
                        _shard.mark_shard_down(idx)
                return total
            shard_idx = _resolve_shard_idx(self.name, query)
            col = _mongo_col_for(self.name, shard_idx)
            if col is not None:
                try:
                    if query:
                        return await col.count_documents(query)
                    return await col.estimated_document_count()
                except Exception as e:
                    print(f"[DB:mongo] count_documents error {self.name} (shard{shard_idx}): {e}")
                    _shard.mark_shard_down(shard_idx)
            conn = await _get_sqlite_for_shard(shard_idx)
        else:
            conn = await _get_sqlite()
        tbl  = _tbl(self.name)
        await _ensure_table(conn, self.name)
        if not query:
            async with conn.execute(f"SELECT COUNT(*) FROM {tbl}") as cur:
                row = await cur.fetchone()
                return row[0] if row else 0
        async with conn.execute(f"SELECT data FROM {tbl}") as cur:
            rows = await cur.fetchall()
        return sum(
            1 for row in rows
            if _match(_loads(row["data"]), query)
        )

    # ── create_index ──────────────────────────────────────────────────────────

    async def create_index(
        self,
        keys,
        unique: bool = False,
        sparse: bool = False,
        expireAfterSeconds: int | None = None,
    ):
        """
        SQLite: no-op (tidak perlu index eksplisit).
        MongoDB: buat index asli via motor — di SEMUA shard yang relevan untuk
        collection ini (penting untuk TTL index seperti bio_profiles.expires_at,
        agar auto-expire bekerja konsisten di tiap cluster, bukan cuma shard 0).
        """
        if _BACKEND == "mongo":
            try:
                from pymongo import ASCENDING, DESCENDING  # type: ignore
                if isinstance(keys, str):
                    keys = [(keys, ASCENDING)]
                bare = _bare_collection_name(self.name)
                shard_range = range(_shard.SHARD_COUNT) if (bare in _shard.SHARDED_COLLECTIONS and _shard.SHARD_COUNT > 1) else [0]
                for idx in shard_range:
                    col = _mongo_col_for(self.name, idx)
                    if col is None:
                        continue
                    try:
                        await col.create_index(
                            keys,
                            unique=unique,
                            sparse=sparse,
                            expireAfterSeconds=expireAfterSeconds,
                        )
                    except Exception:
                        pass
            except Exception:
                pass


# ══════════════════════════════════════════════════════════════════════════════
# DB — dict-like container, mimic motor client["db"]["collection"]
# ══════════════════════════════════════════════════════════════════════════════

class DB:
    def __getitem__(self, name: str) -> Collection:
        return Collection(_ns(name))


db = DB()

# ── Named collections (backward compat) ───────────────────────────────────────
config_db          = db["status"]
messages_db        = db["seen_messages"]
regex_db           = db["regex_list"]
nexus_kalimat_db   = db["nexus_kalimat"]
nexus_regex_db     = db["nexus_regex"]
nexus_grup_db      = db["nexus_grup"]
nexus_whitelist_db = db["nexus_whitelist"]
nexus_actlog_db    = db["nexus_actlog"]
group_action_log_db = db["group_action_log"]
bot_config_db      = db["bot_config"]
mention_cache_db   = db["mention_member_cache"]
mention_global_db  = db["mention_global_cache"]   # non-akun & channel/grup — lintas semua grup
mention_wl_db      = db["mention_whitelist"]       # whitelist per grup (username raw)


# ══════════════════════════════════════════════════════════════════════════════
# HANDLED MESSAGES TRACKER
# ══════════════════════════════════════════════════════════════════════════════

def mark_message_handled(chat_id: int, msg_id: int) -> None:
    _handled_msgs[(chat_id, msg_id)] = time.time()
    if len(_handled_msgs) > 2000:
        cutoff = time.time() - _HANDLED_TTL
        stale  = [k for k, ts in _handled_msgs.items() if ts < cutoff]
        for k in stale:
            _handled_msgs.pop(k, None)


def is_message_handled(chat_id: int, msg_id: int) -> bool:
    key = (chat_id, msg_id)
    ts  = _handled_msgs.get(key)
    if ts is None:
        return False
    if time.time() - ts > _HANDLED_TTL:
        _handled_msgs.pop(key, None)
        return False
    return True


# ══════════════════════════════════════════════════════════════════════════════
# SETUP — init backend + tabel + background cleanup
# ══════════════════════════════════════════════════════════════════════════════

async def _cleanup_seen_messages():
    """Background task: hapus seen_messages lebih dari 24 jam, jalan setiap 1 jam."""
    while True:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            break
        try:
            cutoff = time.time() - 86400
            if _BACKEND == "mongo":
                # PENTING: pakai Collection (bukan _mongo_db langsung) supaya
                # cleanup menjangkau SEMUA shard tempat seen_messages tersebar,
                # bukan hanya shard 0. _ns() tetap diterapkan oleh db[...].
                await db["seen_messages"].delete_many({"time": {"$lt": cutoff}})
            else:
                conn = await _get_sqlite()
                # _ns() sudah diterapkan saat tabel dibuat di setup_db(); pakai nama yang sama
                ns_col = _ns("seen_messages")
                tbl    = _tbl(ns_col)
                await _ensure_table(conn, ns_col)
                async with conn.execute(f"SELECT id, data FROM {tbl}") as cur:
                    rows = await cur.fetchall()
                deleted = 0
                for row in rows:
                    try:
                        d  = _loads(row["data"])
                        ts = d.get("time", 0)
                        if isinstance(ts, (int, float)) and ts < cutoff:
                            await conn.execute(f"DELETE FROM {tbl} WHERE id=?", (row["id"],))
                            deleted += 1
                    except Exception:
                        pass
                if deleted:
                    await conn.commit()
                    prefix = f"[{_CODE_BOT}] " if _CODE_BOT else ""
                    print(f"[DB] {prefix}cleanup: {deleted} seen_messages expired dihapus")
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[DB] cleanup error: {e}")



async def _migrate_legacy_data():
    """
    Migrasi otomatis data lama (tanpa CODE_BOT prefix) ke namespace aktif.
    Dijalankan sekali saat startup jika CODE_BOT aktif.
    Hanya menyalin dokumen yang BELUM ada di namespace baru — tidak menimpa.
    Aman dijalankan berulang kali.
    """
    if not _CODE_BOT:
        return

    # Daftar semua collection yang perlu dicek
    _COLLECTIONS = [
        "status", "seen_messages", "regex_list",
        "regex_per_group", "whitelist_per_group", "free_per_group",
        "nexus_kalimat", "nexus_regex", "nexus_grup", "nexus_whitelist",
        "nexus_actlog", "local_mute", "group_action_log",
        "ai_debug_log", "dm_users", "nexus_ai_model", "bot_config",
    ]

    migrated_total = 0

    if _BACKEND == "mongo":
        # Catatan: migrasi legacy ini SENGAJA hanya menyentuh shard 0 (_mongo_db
        # variable lama = shard utama). Data lama (sebelum sharding ada) pasti
        # semua berada di shard 0, jadi tidak perlu fan-out ke shard lain.
        for col_name in _COLLECTIONS:
            old_col = _mongo_db[col_name]          # collection lama tanpa prefix
            new_col = _mongo_db[_ns(col_name)]     # collection baru dengan prefix

            # Skip jika nama sama (tidak ada prefix)
            if col_name == _ns(col_name):
                continue

            try:
                old_count = await old_col.count_documents({})
                if old_count == 0:
                    continue

                new_count = await new_col.count_documents({})
                if new_count > 0:
                    # Sudah ada data di namespace baru, skip
                    continue

                # Copy semua dokumen dari old ke new
                docs = []
                async for doc in old_col.find({}):
                    docs.append(doc)

                if docs:
                    try:
                        await new_col.insert_many(docs, ordered=False)
                        migrated_total += len(docs)
                        print(f"[Migrasi] {col_name} → {_ns(col_name)}: {len(docs)} dokumen dipindah")
                    except Exception as e:
                        print(f"[Migrasi] {col_name}: sebagian gagal ({e})")

            except Exception as e:
                print(f"[Migrasi] Error cek {col_name}: {e}")

    elif _BACKEND == "sqlite":
        conn = await _get_sqlite()
        for col_name in _COLLECTIONS:
            new_col = _ns(col_name)
            if col_name == new_col:
                continue

            try:
                # Cek apakah tabel lama ada
                async with conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (_tbl(col_name),)
                ) as cur:
                    row = await cur.fetchone()
                if not row:
                    continue

                # Cek jumlah data di tabel lama
                async with conn.execute(f"SELECT COUNT(*) FROM {_tbl(col_name)}") as cur:
                    old_count = (await cur.fetchone())[0]
                if old_count == 0:
                    continue

                # Cek tabel baru sudah ada data?
                await _ensure_table(conn, new_col)
                async with conn.execute(f"SELECT COUNT(*) FROM {_tbl(new_col)}") as cur:
                    new_count = (await cur.fetchone())[0]
                if new_count > 0:
                    continue

                # Copy
                async with conn.execute(f"SELECT data FROM {_tbl(col_name)}") as cur:
                    rows = await cur.fetchall()

                for row in rows:
                    try:
                        await conn.execute(
                            f"INSERT OR IGNORE INTO {_tbl(new_col)} (id, data) VALUES (?, ?)",
                            (str(uuid.uuid4()), row["data"] if isinstance(row, dict) else row[0])
                        )
                    except Exception:
                        pass

                await conn.commit()
                migrated_total += old_count
                print(f"[Migrasi] SQLite {col_name} → {new_col}: {old_count} baris dipindah")

            except Exception as e:
                print(f"[Migrasi] SQLite error {col_name}: {e}")

    if migrated_total > 0:
        print(f"[Migrasi] ✅ Total {migrated_total} dokumen berhasil dimigrasikan ke namespace [{_CODE_BOT}]")
    else:
        print(f"[Migrasi] ✅ Namespace [{_CODE_BOT}] sudah up-to-date, tidak ada migrasi diperlukan")


async def _migrate_sqlite_to_mongo():
    """
    Migrasi data dari SQLite lokal ke MongoDB saat backend aktif adalah MongoDB
    dan file SQLite lokal masih ada dan berisi data.

    Alur:
      1. Cek apakah file SQLite ada dan tidak kosong.
      2. Untuk setiap collection, ambil semua dokumen dari SQLite.
      3. Untuk setiap dokumen, cek apakah sudah ada di MongoDB (berdasarkan _id atau doc_id).
         - Jika belum ada → insert ke MongoDB.
         - Jika sudah ada (duplikat) → skip (data MongoDB diutamakan).
      4. Setelah semua collection selesai dan SQLite sudah kosong total → log selesai.
    """
    import os as _os
    import json as _json

    if _BACKEND != "mongo" or _mongo_db is None:
        return  # Hanya jalan jika backend aktif adalah MongoDB

    sqlite_path = SQLITE_PATH
    if not _os.path.exists(sqlite_path):
        return  # Tidak ada file SQLite, skip

    # Cek apakah file SQLite punya data sama sekali
    try:
        import aiosqlite as _aiosqlite
        async with _aiosqlite.connect(sqlite_path) as sq:
            sq.row_factory = _aiosqlite.Row
            async with sq.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ) as cur:
                tables = [r[0] for r in await cur.fetchall()]
        if not tables:
            return  # SQLite kosong, skip
    except Exception as e:
        print(f"[Migrasi SQLite→Mongo] Gagal buka SQLite: {e}")
        return

    _COLLECTIONS = [
        "status", "seen_messages", "regex_list",
        "regex_per_group", "whitelist_per_group", "free_per_group",
        "nexus_kalimat", "nexus_regex", "nexus_grup", "nexus_whitelist",
        "local_mute", "group_action_log",
        "nexus_actlog", "ai_debug_log", "dm_users", "nexus_ai_model", "bot_config",
    ]

    total_migrated = 0
    total_skipped  = 0

    print("[Migrasi SQLite→Mongo] 🔄 Ditemukan data SQLite lokal, mulai migrasi...")

    try:
        async with _aiosqlite.connect(sqlite_path) as sq:
            sq.row_factory = _aiosqlite.Row

            for col_name in _COLLECTIONS:
                # Coba kedua kemungkinan nama tabel: dengan prefix dan tanpa prefix
                candidates = list({_tbl(_ns(col_name)), _tbl(col_name)})
                for tbl in candidates:
                    # Cek apakah tabel ini ada di SQLite
                    async with sq.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,)
                    ) as cur:
                        exists = await cur.fetchone()
                    if not exists:
                        continue

                    async with sq.execute(f"SELECT id, doc_id, data FROM {tbl}") as cur:
                        rows = await cur.fetchall()

                    if not rows:
                        continue

                    mongo_col = _mongo_db[_ns(col_name)]
                    inserted  = 0
                    skipped   = 0

                    for row in rows:
                        try:
                            raw = row["data"] if "data" in row.keys() else None
                            if not raw:
                                continue
                            doc = _json.loads(raw) if isinstance(raw, str) else raw

                            # Tentukan _id untuk cek duplikat
                            doc_id = row["doc_id"] if "doc_id" in row.keys() else None
                            if doc_id:
                                doc.setdefault("_id", doc_id)

                            filter_q = {"_id": doc["_id"]} if "_id" in doc else None

                            if filter_q:
                                existing = await mongo_col.find_one(filter_q)
                                if existing:
                                    skipped += 1
                                    continue

                            await mongo_col.insert_one(doc)
                            inserted += 1

                        except Exception:
                            skipped += 1
                            continue

                    if inserted:
                        print(f"[Migrasi SQLite→Mongo] ✅ {tbl} → {_ns(col_name)}: {inserted} dokumen dipindah, {skipped} duplikat dilewati")
                    total_migrated += inserted
                    total_skipped  += skipped

    except Exception as e:
        print(f"[Migrasi SQLite→Mongo] ❌ Error: {e}")
        return

    if total_migrated > 0:
        print(f"[Migrasi SQLite→Mongo] ✅ Selesai. Total {total_migrated} dokumen dipindah, {total_skipped} duplikat dilewati.")
        print(f"[Migrasi SQLite→Mongo] ℹ️  File SQLite ({sqlite_path}) tetap ada sebagai backup.")
        print(f"[Migrasi SQLite→Mongo] ℹ️  Hapus manual jika sudah yakin data aman di MongoDB.")
    else:
        print(f"[Migrasi SQLite→Mongo] ✅ Semua data SQLite sudah ada di MongoDB ({total_skipped} duplikat). Tidak ada yang perlu dipindah.")


async def _create_panel_indexes() -> None:
    """
    Buat index untuk koleksi yang dipakai berulang dari tombol-tombol panel
    grup (chat_id sebagai filter utama, beberapa juga user_id).

    Idempotent — aman dipanggil tiap startup. No-op total di SQLite
    (lihat implementasi Collection.create_index). TIDAK mengubah query,
    hasil, maupun urutan logika apapun di kode lain — index hanya
    membuat MongoDB menemukan dokumen yang sama jauh lebih cepat,
    tanpa full collection scan lintas semua grup setiap kali tombol
    panel grup diklik.
    """
    if _BACKEND != "mongo":
        return
    try:
        from pymongo import ASCENDING  # type: ignore

        # status (config_db) — dibaca tiap kali panel grup manapun dibuka
        # (get_config), sekalipun ada cache TTL in-memory di atasnya.
        await config_db.create_index([("chat_id", ASCENDING)])

        # regex_per_group — panel utama (hitung jumlah filter) & daftar regex
        await db["regex_per_group"].create_index([("chat_id", ASCENDING)])

        # free_per_group — panel utama (hitung VIP) & daftar Member VIP,
        # juga dicek per (chat_id, user_id) di banyak filter pesan.
        await db["free_per_group"].create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)])

        # whitelist_per_group — panel CAS & daftar whitelist,
        # juga dicek per (chat_id, user_id) di filter CAS.
        await db["whitelist_per_group"].create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)])

        # security_os — dibaca _sec_os_get setiap render/toggle panel Security OS
        await db["security_os"].create_index([("chat_id", ASCENDING)])

        # vc_muted_by_ub — dicek tiap /unmutemic dan tiap siklus scan VC
        await db["vc_muted_by_ub"].create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)])

        # seen_messages (messages_db) — dicek SETIAP pesan yang lolos sampai
        # tahap Anti Duplikasi Lokal (core/antispam_queue.py), baik untuk
        # cari pesan lama yang mirip (find().sort("time",-1).limit(N)) maupun
        # untuk hitung total histori user guna pembersihan (all_docs).
        # TANPA index ini, query itu full collection scan — makin lambat
        # makin besar koleksinya (koleksi ini diisi TIAP pesan bersih juga,
        # bukan cuma duplikat, jadi tumbuh terus menerus).
        # Compound index (chat_id, user_id, type, time) mencakup filter
        # SEKALIGUS urutan sort by time tanpa sort tambahan di memori.
        await db["seen_messages"].create_index([
            ("chat_id", ASCENDING), ("user_id", ASCENDING),
            ("type", ASCENDING), ("time", ASCENDING),
        ])
    except Exception as e:
        print(f"[DB] Gagal buat index panel: {e}")


async def setup_db():
    """
    Inisialisasi backend (MongoDB atau SQLite) dan mulai background cleanup.
    Wajib dipanggil sekali di antigcast.py saat startup.
    """
    await _init_backend()

    if _BACKEND == "sqlite":
        conn = await _get_sqlite()
        for col_name in [
            "status", "seen_messages", "regex_list",
            "regex_per_group", "whitelist_per_group", "free_per_group",
            "nexus_kalimat", "nexus_regex", "nexus_grup", "nexus_whitelist",
            "local_mute", "group_action_log",
            "nexus_actlog", "ai_debug_log", "dm_users", "nexus_ai_model", "bot_config",
            "security_os", "security_os_monitors",
            "mention_member_cache",
        ]:
            await _ensure_table(conn, _ns(col_name))

    # ── Migrasi SQLite lokal → MongoDB (jika backend aktif Mongo & SQLite ada) ─
    await _migrate_sqlite_to_mongo()

    asyncio.create_task(_cleanup_seen_messages())

    # ── Migrasi data lama (tanpa CODE_BOT prefix) ke namespace aktif ─────────
    if _CODE_BOT:
        await _migrate_legacy_data()

    # ── Index performa: koleksi yang dipakai berulang dari tombol panel ─────
    # FIX (tombol panel terasa berat): koleksi-koleksi ini di-query dengan
    # filter chat_id (dan/atau user_id) setiap kali tombol grup terkait
    # diklik (panel utama, regex, whitelist, free/VIP, Security OS, dll),
    # tapi tidak punya index sama sekali — di MongoDB artinya full collection
    # scan lintas SEMUA grup setiap klik. Penambahan index ini TIDAK
    # mengubah hasil/logika apapun, hanya membuat query yang SAMA jadi
    # lebih cepat dicari oleh database. Idempotent & no-op di SQLite
    # (lihat Collection.create_index).
    await _create_panel_indexes()

    # ── Banner detail startup ─────────────────────────────────────────────────
    sep = "─" * 52
    print(f"\n╔{sep}╗")
    print(f"║{'  DATABASE INFO':^52}║")
    print(f"╠{sep}╣")

    if _BACKEND == "mongo":
        url_display = MONGO_URL[:45] + "…" if len(MONGO_URL) > 45 else MONGO_URL
        print(f"║  Backend   : MongoDB (cloud/server)              ║")
        print(f"║  URL       : {url_display:<39}║")
        print(f"║  DB Name   : {MONGO_DB_NAME:<39}║")
    else:
        abs_path = os.path.abspath(SQLITE_PATH)
        path_display = abs_path[-45:] if len(abs_path) > 45 else abs_path
        print(f"║  Backend   : SQLite (lokal / Termux)             ║")
        print(f"║  File      : {path_display:<39}║")

    print(f"╠{sep}╣")

    if _CODE_BOT:
        print(f"║  CODE_BOT  : [{_CODE_BOT}]")
        print(f"║  Namespace : semua koleksi pakai prefix [{_CODE_BOT}_…]")
        print(f"║  Akses DB  : bot lain dengan CODE_BOT sama")
        print(f"║              → berbagi data yang sama ✅")
        print(f"║              bot lain dengan CODE_BOT beda")
        print(f"║              → data terpisah, tidak campur ✅")
    else:
        print(f"║  CODE_BOT  : (tidak diset / kosong)")
        print(f"║  ⚠️  PERINGATAN: tanpa CODE_BOT, semua bot yang")
        print(f"║     pakai DB yang sama akan BERBAGI data!")
        print(f"║     Isi CODE_BOT di .env untuk isolasi data.")

    print(f"╚{sep}╝\n")


async def save_bot_config(key: str, value) -> None:
    """
    Simpan setting bot ke DB secara persisten.
    Dipakai untuk cache info channel (title, username) agar dikenal lintas sesi.
    """
    try:
        await bot_config_db.update_one(
            {"_id": key},
            {"$set": {"_id": key, "value": value}},
            upsert=True,
        )
    except Exception as e:
        print(f"[DB] save_bot_config error ({key}): {e}")


async def get_bot_config(key: str, default=None):
    """Ambil setting bot dari DB. Return default jika tidak ada."""
    try:
        doc = await bot_config_db.find_one({"_id": key})
        if doc is not None:
            return doc.get("value", default)
    except Exception as e:
        print(f"[DB] get_bot_config error ({key}): {e}")
    return default


async def reset_code_bot_data(code_bot: str) -> tuple[int, list[str]]:
    """
    Hapus semua data dari namespace CODE_BOT yang diberikan.
    Cocok untuk perintah /reset — membersihkan SEMUA data satu bot.

    Return: (total_dokumen_dihapus, daftar_koleksi_yang_dibersihkan)
    """
    import re as _re2
    safe   = _re2.sub(r"[^a-zA-Z0-9]", "_", code_bot.strip()).lower().strip("_")
    prefix = f"{safe}_" if safe else ""

    _ALL_COLS = [
        "status", "seen_messages", "regex_list",
        "regex_per_group", "whitelist_per_group", "free_per_group",
        "nexus_kalimat", "nexus_regex", "nexus_grup", "nexus_whitelist",
        "nexus_actlog", "local_mute", "group_action_log",
        "ai_debug_log", "dm_users", "nexus_ai_model", "bot_config",
    ]

    cleared: list[str] = []
    total   = 0

    if _BACKEND == "mongo":
        for col_name in _ALL_COLS:
            ns = f"{prefix}{col_name}" if prefix else col_name
            try:
                # Pakai Collection (lewat DB.__getitem__ tanpa _ns ganda — ns
                # di sini SUDAH final) supaya delete_many fan-out ke SEMUA
                # shard untuk collection yang sharded (bio_profiles dkk),
                # bukan hanya shard 0. _resolve_shard_idx menerima query={}
                # yang berarti "tanpa chat_id" → otomatis fan-out semua shard.
                r = await Collection(ns).delete_many({})
                if r.deleted_count > 0:
                    total += r.deleted_count
                    cleared.append(f"{ns} ({r.deleted_count})")
            except Exception as e:
                print(f"[reset] MongoDB error {ns}: {e}")

    elif _BACKEND == "sqlite":
        conn = await _get_sqlite()
        for col_name in _ALL_COLS:
            ns  = f"{prefix}{col_name}" if prefix else col_name
            tbl = _tbl(ns)
            try:
                async with conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,)
                ) as cur:
                    row = await cur.fetchone()
                if not row:
                    continue
                async with conn.execute(f"SELECT COUNT(*) FROM {tbl}") as cur:
                    cnt = (await cur.fetchone())[0]
                await conn.execute(f"DELETE FROM {tbl}")
                if cnt > 0:
                    total += cnt
                    cleared.append(f"{ns} ({cnt})")
            except Exception as e:
                print(f"[reset] SQLite error {ns}: {e}")
        await conn.commit()

    # Bersihkan cache in-memory jika namespace aktif yang direset
    if safe == _CODE_BOT:
        _config_cache.clear()
        _admin_cache.clear()

    return total, cleared


async def close_db():
    """Tutup koneksi database dengan bersih saat shutdown."""
    global _sqlite_conn, _mongo_db
    if _BACKEND == "sqlite" and _sqlite_conn is not None:
        try:
            await _sqlite_conn.close()
            _sqlite_conn = None
            print("[DB] SQLite connection ditutup.")
        except Exception as e:
            print(f"[DB] Error tutup SQLite: {e}")
    elif _BACKEND == "mongo" and _mongo_db is not None:
        try:
            _mongo_db.client.close()
            _mongo_db = None
            print("[DB] MongoDB connection ditutup.")
        except Exception as e:
            print(f"[DB] Error tutup MongoDB: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# CONFIG HELPERS
# ══════════════════════════════════════════════════════════════════════════════

async def get_config(chat_id: int) -> dict:
    now = time.monotonic()
    hit = _config_cache.get(chat_id)
    if hit and (now - hit[1]) < CONFIG_TTL:
        return hit[0]
    doc = await config_db.find_one({"chat_id": chat_id})
    cfg = dict(DEFAULT_CONFIG)
    if doc:
        for k in DEFAULT_CONFIG:
            if k in doc:
                cfg[k] = doc[k]
    _config_cache[chat_id] = (cfg, now)
    return cfg


async def update_config(chat_id: int, key: str, value) -> None:
    await config_db.update_one(
        {"chat_id": chat_id},
        {"$set": {"chat_id": chat_id, key: value}},
        upsert=True,
    )
    _config_cache.pop(chat_id, None)


def update_config_optimistic(
    chat_id: int, key: str, value,
    dm_chat_id: int | None = None, dm_msg_id: int | None = None,
) -> dict:
    """
    Versi "ringan" dari update_config — dipakai oleh tombol panel DM.

    1. Cache di-update LANGSUNG (synchronous) → panggilan get_config()
       berikutnya (dipakai untuk render ulang panel) langsung melihat
       nilai baru tanpa menunggu DB.
    2. Penulisan sesungguhnya ke DB diantrikan via panel_write_queue dan
       dieksekusi belakangan oleh panel_write_worker.
    3. Jika dm_chat_id + dm_msg_id diisi (lokasi pesan panel di DM admin)
       dan penulisan ternyata GAGAL PERMANEN setelah di-retry, worker akan
       mengoreksi tampilan panel itu kembali ke nilai DB yang sebenarnya.

    Return dict config terbaru (hasil optimistic) agar pemanggil bisa
    langsung pakai untuk render tanpa query ulang.
    """
    now = time.monotonic()
    hit = _config_cache.get(chat_id)
    cfg = dict(hit[0]) if hit else dict(DEFAULT_CONFIG)
    cfg[key] = value
    _config_cache[chat_id] = (cfg, now)
    enqueue_config_write(chat_id, key, value, dm_chat_id, dm_msg_id)
    return cfg


# ── Cached count helpers (dipakai oleh page_manage di panel DM) ───────────────

async def get_regex_count(chat_id: int) -> int:
    """Count regex rules untuk grup, dengan cache COUNT_TTL detik."""
    now = time.monotonic()
    hit = _regex_count_cache.get(chat_id)
    if hit and (now - hit[1]) < COUNT_TTL:
        return hit[0]
    n = await db["regex_per_group"].count_documents({"chat_id": chat_id})
    _regex_count_cache[chat_id] = (n, now)
    return n


async def get_free_count(chat_id: int) -> int:
    """Count VIP members untuk grup, dengan cache COUNT_TTL detik."""
    now = time.monotonic()
    hit = _free_count_cache.get(chat_id)
    if hit and (now - hit[1]) < COUNT_TTL:
        return hit[0]
    n = await db["free_per_group"].count_documents({"chat_id": chat_id})
    _free_count_cache[chat_id] = (n, now)
    return n


def invalidate_count_cache(chat_id: int) -> None:
    """Hapus cache count untuk grup ini (panggil saat regex/VIP ditambah/hapus)."""
    _regex_count_cache.pop(chat_id, None)
    _free_count_cache.pop(chat_id, None)


def invalidate_admin_groups_cache(user_id: int) -> None:
    """Paksa refresh daftar grup admin (panggil saat tombol Refresh ditekan)."""
    _admin_groups_cache.pop(user_id, None)


def invalidate_nexus_counts() -> None:
    """Hapus semua cache count nexus — panggil setelah operasi tulis ke nexus collections."""
    global _nexus_kalimat_count_cache, _nexus_regex_count_cache
    global _nexus_wl_count_cache, _nexus_owner_regex_count_cache, _nexus_grup_cache
    _nexus_kalimat_count_cache      = None
    _nexus_regex_count_cache        = None
    _nexus_wl_count_cache           = None
    _nexus_owner_regex_count_cache  = None
    _nexus_grup_cache               = None


# ══════════════════════════════════════════════════════════════════════════════
# BOT PERMISSIONS CACHE
# Cek apakah bot punya can_delete_messages DAN can_restrict_members di sebuah
# grup. Cache 5 menit per grup agar tidak terus-terus hit Telegram API.
# Dipakai oleh antispam.py, antispam_queue.py, dan panel DM (handlers_dm.py).
# ══════════════════════════════════════════════════════════════════════════════

_bot_perm_cache: dict[int, tuple[bool, float]] = {}
_BOT_PERM_CACHE_TTL = 300  # 5 menit — cukup sering refresh, tidak terlalu boros API


async def check_bot_permissions(client, chat_id: int) -> bool:
    """
    Kembalikan True jika bot punya KEDUA izin di grup chat_id:
      • can_delete_messages  — wajib untuk hapus pesan spam
      • can_restrict_members — wajib untuk ban/mute/restrict user

    Jika salah satu tidak ada → False → bot harus skip grup ini sepenuhnya.

    Cache 5 menit per grup. Saat gagal query ke Telegram (error jaringan, dll.)
    → fail-open (return True) agar bot tidak tiba-tiba berhenti di semua grup
    hanya karena Telegram sedang lambat.
    """
    now = _time_module.monotonic()
    cached = _bot_perm_cache.get(chat_id)
    if cached:
        has_perms, ts = cached
        if now - ts < _BOT_PERM_CACHE_TTL:
            return has_perms

    try:
        me     = client.me
        member = await client.get_chat_member(chat_id, me.id)
        privs  = getattr(member, "privileges", None)
        if privs is None:
            # Bot tidak punya privileges objek → bukan admin
            has_perms = False
        else:
            can_del      = getattr(privs, "can_delete_messages",  False) or False
            can_restrict = getattr(privs, "can_restrict_members", False) or False
            has_perms    = bool(can_del and can_restrict)
    except Exception as e:
        print(f"[BotPerm] Gagal cek izin chat={chat_id}: {e} — anggap OK (fail-open)")
        has_perms = True  # fail-open: jangan block bot saat Telegram error

    _bot_perm_cache[chat_id] = (has_perms, now)
    return has_perms


def invalidate_bot_perm_cache(chat_id: int) -> None:
    """Hapus cache izin bot untuk grup ini (misalnya setelah bot di-promote ulang)."""
    _bot_perm_cache.pop(chat_id, None)


# ══════════════════════════════════════════════════════════════════════════════
# PERM WATCHDOG — pengecekan berkala izin ban/mute per grup (lihat core/perm_watchdog.py)
#
# Berbeda dari check_bot_permissions() (reaktif, dipicu pesan masuk, cache 5 menit,
# fail-open saat error API): watchdog ini PROAKTIF, jalan terus berkala bahkan
# untuk grup yang sepi pesan, dan saat izin ban/mute hilang ia menulis status
# OFF ke DATABASE (bukan cache sesaat) — sehingga toggle panel ikut berubah dan
# kondisinya bertahan walau cache check_bot_permissions kosong/fail-open.
# ══════════════════════════════════════════════════════════════════════════════

blocked_groups_db = db["blocked_groups"]

# Toggle yang menjalankan EKSEKUSI hapus/ban — dipaksa OFF saat kuasa ban/mute
# hilang. TIDAK termasuk: ubot_detect (rekam selalu jalan, independen),
# anti_mention/mention record, dan Nexus AI passive learning — ketiganya
# sudah didesain untuk tetap jalan tanpa izin admin (lihat masing-masing filter).
_PERM_GATED_TOGGLES = ("local", "global", "cas")


async def force_disable_group_moderation(chat_id: int) -> bool:
    """
    Paksa matikan toggle yang berujung pada eksekusi hapus pesan/ban
    (local, global, cas) untuk chat_id, dan tandai grup ini sebagai
    "dimatikan oleh watchdog" (perm_forced_off=True) — bukan oleh admin grup.

    Return True jika ada perubahan (sebelumnya minimal satu toggle ON),
    False jika grup sudah dalam keadaan OFF semua (tidak ada yang berubah).
    """
    cfg = await get_config(chat_id)
    already_off = not any(cfg.get(k) for k in _PERM_GATED_TOGGLES)

    updates = {k: False for k in _PERM_GATED_TOGGLES}
    updates["perm_forced_off"] = True
    await config_db.update_one(
        {"chat_id": chat_id},
        {"$set": {"chat_id": chat_id, **updates}},
        upsert=True,
    )
    _config_cache.pop(chat_id, None)
    invalidate_bot_perm_cache(chat_id)

    if already_off:
        return False
    print(f"[PermWatchdog] ⛔ Grup {chat_id}: kuasa ban/mute hilang — "
          f"local/global/cas dipaksa OFF.")
    return True


async def restore_group_moderation_if_forced(chat_id: int) -> bool:
    """
    Nyalakan kembali local/global ke True HANYA jika grup ini sebelumnya
    dimatikan oleh watchdog (perm_forced_off=True) — tidak menimpa pilihan
    admin grup yang mematikan fitur secara manual.

    `cas` SENGAJA TIDAK di-restore otomatis — CAS adalah auto-ban global
    yang lebih sensitif, biar admin grup yang menyalakan ulang secara sadar
    lewat panel setelah memverifikasi izin bot benar-benar pulih.

    Return True jika grup ini di-restore, False jika tidak perlu (tidak
    pernah di-force-off oleh watchdog).
    """
    # PENTING: jangan pakai get_config() di sini — get_config() hanya
    # menyalin field yang ada di DEFAULT_CONFIG, dan "perm_forced_off"
    # SENGAJA tidak dimasukkan ke DEFAULT_CONFIG (bukan toggle fitur).
    # Akibatnya cfg.get("perm_forced_off") selalu None/False meski nilai
    # asli di DB True, sehingga fungsi ini selalu return False tanpa
    # pernah benar-benar clear flag-nya. Baca langsung dari config_db.
    doc = await config_db.find_one({"chat_id": chat_id})
    if not doc or not doc.get("perm_forced_off"):
        return False

    await config_db.update_one(
        {"chat_id": chat_id},
        {"$set": {"local": True, "global": True, "perm_forced_off": False}},
    )
    _config_cache.pop(chat_id, None)
    print(f"[PermWatchdog] ✅ Grup {chat_id}: kuasa ban/mute pulih — "
          f"local/global diaktifkan kembali (cas tetap OFF, perlu manual).")
    return True


async def block_and_remove_group(chat_id: int, reason: str) -> None:
    """
    Grup tidak ditemukan lagi (bot dikick / grup mati / dihapus). Hapus data
    operasionalnya dari config_db (lewat remove_group_data) DAN catat
    permanen ke blocked_groups agar grup ini tidak diam-diam terdaftar lagi
    kalau suatu saat bot diundang balik tanpa sepengetahuan owner.
    """
    await blocked_groups_db.update_one(
        {"chat_id": chat_id},
        {"$set": {
            "chat_id":    chat_id,
            "reason":     reason,
            "blocked_at": datetime.now(TZ_WIB),
        }},
        upsert=True,
    )
    await remove_group_data(chat_id)
    print(f"[PermWatchdog] 🚫 Grup {chat_id} diblokir & dihapus dari DB ({reason}).")


async def is_group_blocked(chat_id: int) -> bool:
    """True jika chat_id ada di daftar blocked_groups."""
    doc = await blocked_groups_db.find_one({"chat_id": chat_id})
    return doc is not None


async def get_all_known_group_ids() -> list[int]:
    """
    Kembalikan semua chat_id grup yang dikenal di config_db (bukan channel),
    dipakai watchdog untuk iterasi bergilir. Channel (chat_type=CHANNEL
    tersimpan) dikecualikan karena tidak relevan dengan moderasi grup.
    """
    ids: list[int] = []
    async for doc in config_db.find({}):
        chat_id = doc.get("chat_id")
        if not chat_id:
            continue
        if (doc.get("chat_type") or "").upper() == "CHANNEL":
            continue
        ids.append(chat_id)
    return ids


async def get_all_groups_with_perm_status() -> list[dict]:
    """
    Kembalikan SEMUA grup yang memakai bot (sumber: config_db — bukan
    nexus_grup_db yang isinya hanya grup yang pernah trigger /spam atau
    event member-update), beserta status izin ban/mute TERKINI.

    Dipakai oleh panel "Grup Terdaftar" (Nexus AI > Owner Bot) supaya owner
    tidak melihat daftar yang menyesatkan — grup tanpa izin ban/mute (sudah
    di-force-off oleh perm_watchdog) HARUS terlihat statusnya, bukan tampil
    polos seolah normal.

    Grup yang sudah mati/bot-dikick TIDAK akan muncul di sini sama sekali,
    karena perm_watchdog menghapusnya dari config_db (lihat
    block_and_remove_group) — daftar ini otomatis hanya berisi grup yang
    masih aktif memakai bot saat ini.

    Channel dikecualikan (sama seperti get_all_known_group_ids).

    Setiap item:
      {
        "chat_id":         int,
        "title":           str,
        "username":        str | None,
        "invite_link":     str | None,  # hanya terisi untuk grup privat
        "has_ban_perm":    bool,  # status TERKINI (perm_forced_off==False)
        "forced_off":      bool,  # True jika watchdog yang mematikan, bukan admin
      }
    """
    result: list[dict] = []
    async for doc in config_db.find({}):
        chat_id = doc.get("chat_id")
        if not chat_id:
            continue
        if (doc.get("chat_type") or "").upper() == "CHANNEL":
            continue

        forced_off   = bool(doc.get("perm_forced_off", False))
        result.append({
            "chat_id":      chat_id,
            "title":        doc.get("title") or str(chat_id),
            "username":     doc.get("username"),
            "invite_link":  doc.get("invite_link"),
            "has_ban_perm": not forced_off,
            "forced_off":   forced_off,
        })
    return result


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN CACHE
# ══════════════════════════════════════════════════════════════════════════════

async def is_admin(client, chat_id: int, user_id) -> bool:
    if not user_id:
        return False
    now = time.monotonic()
    key = (chat_id, user_id)
    hit = _admin_cache.get(key)
    if hit and (now - hit[1]) < ADMIN_TTL:
        return hit[0]
    try:
        member = await client.get_chat_member(chat_id, user_id)
        result = member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    except Exception as e:
        result = False
    _admin_cache[key] = (result, now)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# AUTO DELETE / DELETE WORKER
# ══════════════════════════════════════════════════════════════════════════════

async def auto_delete_reply(msgs: list, delay: int = 5) -> None:
    """
    Hapus pesan setelah `delay` detik.

    FIXED: Sebelumnya memanggil m.delete() satu per satu di sini, yang
    artinya setiap coroutine yang 'await auto_delete_reply(...)' tidur
    selama delay detik lalu menembak N delete calls individual setelah
    bangun — jika banyak handler bangun bersamaan (raid CAS, settings
    beruntun), hasilnya burst ke API.

    Sekarang: setelah tidur, pesan dikelompokkan per chat_id dan dikirim
    sekaligus ke delete_queue (worker yang sudah ada). delete_worker akan
    menggabungkannya menjadi 1 call delete_messages per chat — aman dari flood.
    """
    await asyncio.sleep(delay)
    # Kelompokkan per chat agar bisa dimasukkan sebagai 1 item queue per chat
    grouped: dict[int, list[int]] = {}
    for m in msgs:
        try:
            cid = m.chat.id
            mid = m.id
            grouped.setdefault(cid, []).append(mid)
        except Exception:
            pass
    for cid, mids in grouped.items():
        try:
            await delete_queue.put((cid, mids))
        except Exception:
            pass


async def delete_worker(client) -> None:
    """
    Worker hapus pesan berbasis per-grup scheduler dengan slot otomatis.

    ARSITEKTUR:
      - 1 dispatcher: baca delete_queue, routing ke bucket per cid.
      - Tiap cid aktif punya 1 asyncio.Task scheduler independen.
      - Slot tiap grup = DELETE_INTERVAL / (jumlah_running + 1)
        → +1 spare agar grup baru langsung dapat slot kosong tanpa tabrakan.
      - Saat grup baru masuk atau grup di-drop → respawn semua task
        dengan slot yang dihitung ulang dari jumlah task running saat itu.
      - Grup dianggap idle jika bucket kosong selama DELETE_IDLE_SECS detik
        berturut-turut → dicopot dari hitungan → trigger respawn.
      - Jika pending grup = 0 saat jadwal tiba → skip, tidak tembak API.
      - Jika pending grup > 0 → langsung delete_messages, apapun keadaannya.
      - Antar grup tidak saling tunggu — jadwal tiap grup independen.

    ENV:
      DELETE_INTERVAL     default 1.0  — total window 1 siklus (detik)
      DELETE_IDLE_SECS    default 30   — berapa detik bucket kosong berturut sebelum drop
      DELETE_BATCH_WINDOW default 0.05 — window kumpul spam sebelum dispatch (detik)
      DELETE_IDLE_TIMEOUT default 5.0  — poll dispatcher saat queue kosong (detik)
    """
    _DELETE_INTERVAL     = float(os.environ.get("DELETE_INTERVAL",     1.0))
    _DELETE_IDLE_SECS    = float(os.environ.get("DELETE_IDLE_SECS",    30.0))
    _BATCH_WINDOW        = float(os.environ.get("DELETE_BATCH_WINDOW", 0.05))
    _IDLE_TIMEOUT        = float(os.environ.get("DELETE_IDLE_TIMEOUT", 5.0))
    _MAX_PENDING_PER_GROUP = 10

    # Tunggu sampai client benar-benar terkoneksi sebelum mulai memproses
    for _ in range(60):
        if getattr(client, "is_connected", False):
            break
        await asyncio.sleep(1.0)

    print("[delete_worker] ✅ Dispatcher siap.", flush=True)

    buckets:     dict[int, list[int]]    = {}  # cid → pending mids
    idle_since: dict[int, float]         = {}  # cid → timestamp pertama kali bucket kosong
    group_tasks: dict[int, asyncio.Task] = {}  # cid → task scheduler aktif

    def _add_to_bucket(cid: int, mids: list[int]) -> None:
        bucket = buckets.setdefault(cid, [])
        idle_since.pop(cid, None)  # ada aktivitas → reset idle timer
        bucket.extend(mids)
        if len(bucket) > _MAX_PENDING_PER_GROUP:
            dropped = len(bucket) - _MAX_PENDING_PER_GROUP
            del bucket[:dropped]  # buang yang paling lama, sisakan terbaru
            print(
                f"[delete_worker] chat={cid}: antrian penuh, "
                f"{dropped} pesan lama di-drop (sisakan {_MAX_PENDING_PER_GROUP} terbaru)"
            )

    async def _group_scheduler(cid: int, slot_offset: float, respawn_event: asyncio.Event) -> None:
        """
        Task independen per grup.
        - Tunggu slot_offset dulu (posisi giliran grup ini dalam siklus).
        - Setelah itu fire tiap _DELETE_INTERVAL detik.
        - Berhenti jika respawn_event di-set (ada respawn global).
        - Idle N cycles berturut → set respawn_event agar dispatcher drop grup ini.
        """
        await asyncio.sleep(slot_offset)

        while not respawn_event.is_set():
            await asyncio.sleep(_DELETE_INTERVAL)
            if respawn_event.is_set():
                break

            mids = buckets.get(cid)
            if not mids:
                # Catat kapan pertama kali kosong
                if cid not in idle_since:
                    idle_since[cid] = time.monotonic()
                elif time.monotonic() - idle_since[cid] >= _DELETE_IDLE_SECS:
                    print(f"[delete_worker] chat={cid}: idle {_DELETE_IDLE_SECS:.0f}s → drop & respawn")
                    respawn_event.set()
                continue

            # Ada pesan → reset idle timer, langsung hapus
            idle_since.pop(cid, None)

            if not getattr(client, "is_connected", False):
                continue

            await wait_global_flood_backoff()
            if respawn_event.is_set():
                break

            to_delete = mids.copy()
            mids.clear()
            try:
                await client.delete_messages(cid, to_delete)
            except asyncio.CancelledError:
                raise
            except Exception as _err:
                from pyrogram.errors import FloodWait as _FW
                if isinstance(_err, _FW):
                    set_global_flood_backoff(_err.value)
                mids[:0] = to_delete  # kembalikan untuk retry interval berikutnya
                print(f"[delete_worker] chat={cid}: gagal delete ({_err}), retry berikutnya")

    def _spawn_all(cids_active: list[int], respawn_event: asyncio.Event) -> None:
        """
        Spawn task scheduler untuk semua cid aktif dengan slot terdistribusi merata.
        slot_size = DELETE_INTERVAL / (n + 1)  → +1 spare untuk grup baru.
        Tiap grup dapat slot_offset = slot_size * index — tidak ada 2 grup fire bersamaan.
        """
        n = len(cids_active)
        slot_size = _DELETE_INTERVAL / (n + 1)
        for i, cid in enumerate(cids_active):
            slot_offset = slot_size * i
            task = asyncio.create_task(
                _group_scheduler(cid, slot_offset, respawn_event),
                name=f"del_sched_{cid}",
            )
            group_tasks[cid] = task
        print(
            f"[delete_worker] 🔄 Respawn {n} grup aktif | "
            f"slot_size={slot_size:.3f}s | spare=1 slot kosong",
            flush=True,
        )

    async def _do_respawn(drop_cid: int | None = None) -> asyncio.Event:
        """
        Cancel semua task lama, copot grup idle jika ada, spawn ulang semua.
        Hanya 1 grup yang di-drop per respawn — yang lain menyusul di siklus berikutnya.
        Return: respawn_event baru untuk siklus berikutnya.
        """
        old_tasks = list(group_tasks.values())
        for t in old_tasks:
            t.cancel()
        if old_tasks:
            await asyncio.gather(*old_tasks, return_exceptions=True)
        group_tasks.clear()

        # Copot 1 grup idle (pemicu respawn ini)
        if drop_cid is not None:
            buckets.pop(drop_cid, None)
            idle_since.pop(drop_cid, None)

        new_event = asyncio.Event()
        active_cids = list(buckets.keys())
        if active_cids:
            _spawn_all(active_cids, new_event)
        return new_event

    # ── Dispatcher loop ───────────────────────────────────────────────────────
    # respawn_event dummy — belum ada task, set agar tidak salah cek awal
    respawn_event: asyncio.Event = asyncio.Event()
    respawn_event.set()

    while True:
        try:
            wait_time = _BATCH_WINDOW if buckets else _IDLE_TIMEOUT
            cid, mids = await asyncio.wait_for(delete_queue.get(), timeout=wait_time)
            is_new_group = cid not in buckets
            _add_to_bucket(cid, mids)
            delete_queue.task_done()

            # Drain semua item yang sudah ada di queue saat ini (non-blocking)
            while not delete_queue.empty():
                try:
                    cid2, mids2 = delete_queue.get_nowait()
                    if cid2 not in buckets:
                        is_new_group = True
                    _add_to_bucket(cid2, mids2)
                    delete_queue.task_done()
                except asyncio.QueueEmpty:
                    break

            # Cek apakah ada task scheduler yang minta respawn (idle drop)
            needs_respawn = respawn_event.is_set()

            if is_new_group or needs_respawn:
                # Cari grup idle yang jadi pemicu respawn (jika ada)
                drop_cid = None
                if needs_respawn:
                    now_mono = time.monotonic()
                    for c, ts in idle_since.items():
                        if now_mono - ts >= _DELETE_IDLE_SECS:
                            drop_cid = c
                            break
                respawn_event = await _do_respawn(drop_cid)

        except asyncio.TimeoutError:
            # Tidak ada item baru — cek apakah ada idle drop yang pending
            if respawn_event.is_set() and buckets:
                drop_cid = None
                now_mono = time.monotonic()
                for c, ts in idle_since.items():
                    if now_mono - ts >= _DELETE_IDLE_SECS:
                        drop_cid = c
                        break
                respawn_event = await _do_respawn(drop_cid)

        except asyncio.CancelledError:
            # Shutdown — cancel semua task grup, flush sisa bucket
            for t in group_tasks.values():
                t.cancel()
            await asyncio.gather(*group_tasks.values(), return_exceptions=True)
            if getattr(client, "is_connected", False):
                for cid, mids in buckets.items():
                    if mids:
                        try:
                            await client.delete_messages(cid, mids)
                        except Exception:
                            pass
            break
        except Exception as _e:
            print(f"[delete_worker] ❌ Dispatcher error: {_e}")
            await asyncio.sleep(0.5)

async def _panel_write_attempt(kind: str, chat_id: int, key, value) -> None:
    """Satu percobaan penulisan ke DB. Lempar exception jika gagal."""
    if kind == "config":
        await config_db.update_one(
            {"chat_id": chat_id},
            {"$set": {"chat_id": chat_id, key: value}},
            upsert=True,
        )
    elif kind == "ns":
        await newscore_cfg_db.update_one(
            {"chat_id": chat_id},
            {"$set": {"chat_id": chat_id, **value}},
            upsert=True,
        )


async def panel_write_worker(client=None) -> None:
    """
    Worker tunggal untuk panel_write_queue.

    Tombol panel DM (toggle on/off, +/- durasi, dsb) sudah mengubah cache
    secara optimistic SEBELUM enqueue di sini — jadi worker ini hanya
    bertugas menulis nilai final ke DB di belakang layar, dengan jeda
    PANEL_WRITE_DELAY detik antar item agar tidak membanjiri DB/API saat
    banyak grup atau banyak klik beruntun terjadi bersamaan.

    Retry & rollback:
      - Tiap item dicoba hingga PANEL_WRITE_RETRIES kali (jeda singkat
        antar percobaan) sebelum dianggap GAGAL PERMANEN.
      - Sukses (kapan pun selama masih dalam batas retry) → selesai,
        tidak ada efek samping lain (silent), karena UI sudah benar.
      - Gagal permanen → cache untuk chat_id itu di-invalidate (paksa
        baca ulang dari DB di akses berikutnya), dan jika item membawa
        lokasi pesan panel (dm_chat_id + dm_msg_id), _panel_rollback_callback
        dipanggil untuk mengoreksi tampilan panel itu balik ke nilai DB
        yang sebenarnya + memberi tahu admin bahwa aksinya tidak tersimpan.

    Item diambil satu per satu (bukan batch) karena tiap toggle bisa
    menyasar koleksi/skema berbeda (config_db vs newscore_cfg_db).
    """
    while True:
        try:
            item = await panel_write_queue.get()
            kind       = item["kind"]
            chat_id    = item["chat_id"]
            key        = item.get("key")
            value      = item["value"]
            dm_chat_id = item.get("dm_chat_id")
            dm_msg_id  = item.get("dm_msg_id")

            ok = False
            last_err = None
            for attempt in range(1, PANEL_WRITE_RETRIES + 1):
                try:
                    await _panel_write_attempt(kind, chat_id, key, value)
                    ok = True
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    last_err = e
                    if attempt < PANEL_WRITE_RETRIES:
                        await asyncio.sleep(0.5 * attempt)  # backoff ringan

            if not ok:
                print(f"[panel_write_worker] GAGAL PERMANEN tulis {kind} {chat_id} {key}: {last_err}")
                # Nilai optimistic yang sempat tersimpan di cache tidak pernah
                # benar-benar mendarat di DB — invalidate agar baca berikutnya
                # ambil nilai asli dari DB, bukan nilai optimistic yang salah.
                if kind == "config":
                    _config_cache.pop(chat_id, None)
                elif kind == "ns":
                    _ns_config_cache.pop(chat_id, None)

                if dm_chat_id and dm_msg_id and _panel_rollback_callback is not None:
                    try:
                        await _panel_rollback_callback(client, kind, chat_id, key, dm_chat_id, dm_msg_id)
                    except Exception as cb_err:
                        print(f"[panel_write_worker] rollback callback gagal: {cb_err}")

            panel_write_queue.task_done()
            await asyncio.sleep(PANEL_WRITE_DELAY)
        except asyncio.CancelledError:
            break
        except Exception:
            # Cegah worker mati diam-diam akibat exception tak terduga
            await asyncio.sleep(0.5)


def enqueue_config_write(
    chat_id: int, key: str, value,
    dm_chat_id: int | None = None, dm_msg_id: int | None = None,
) -> None:
    """Antrikan penulisan satu field config grup ke DB (non-blocking)."""
    panel_write_queue.put_nowait({
        "kind": "config", "chat_id": chat_id, "key": key, "value": value,
        "dm_chat_id": dm_chat_id, "dm_msg_id": dm_msg_id,
    })


def enqueue_ns_write(
    chat_id: int, updates: dict,
    dm_chat_id: int | None = None, dm_msg_id: int | None = None,
) -> None:
    """Antrikan penulisan field(s) NewsCore config grup ke DB (non-blocking)."""
    panel_write_queue.put_nowait({
        "kind": "ns", "chat_id": chat_id, "key": None, "value": updates,
        "dm_chat_id": dm_chat_id, "dm_msg_id": dm_msg_id,
    })


# ══════════════════════════════════════════════════════════════════════════════
# GROUP HELPERS
# ══════════════════════════════════════════════════════════════════════════════

async def get_my_admin_groups(client, user_id: int) -> list:
    """
    Kembalikan semua GRUP (bukan channel) dari config_db (berbagi via CODE_BOT).
    Menggunakan judul tersimpan jika bot token ini tidak ada di grup tersebut,
    sehingga dua bot dengan CODE_BOT yang sama melihat daftar grup yang sama.

    FIX: simpan chat_type saat bisa akses → filter channel saat tidak bisa akses.
    CACHE: hasil di-cache ADMIN_GROUPS_TTL detik — hindari looping Telegram API
           tiap kali admin menekan "Refresh" atau membuka daftar grup.
    """
    now = time.monotonic()
    hit = _admin_groups_cache.get(user_id)
    if hit and (now - hit[1]) < ADMIN_GROUPS_TTL:
        return hit[0]

    from pyrogram.enums import ChatType
    result = []
    async for doc in config_db.find({}):
        chat_id = doc.get("chat_id")
        if not chat_id:
            continue
        title = doc.get("title") or str(chat_id)
        chat_accessible = False
        try:
            chat = await client.get_chat(chat_id)
            if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
                # Channel atau tipe lain → simpan tipe agar filter bekerja saat offline
                await config_db.update_one(
                    {"chat_id": chat_id},
                    {"$set": {"chat_type": chat.type.name}},
                )
                continue
            title = chat.title or title
            chat_accessible = True
            # Perbarui judul + chat_type tersimpan di database
            await config_db.update_one(
                {"chat_id": chat_id},
                {"$set": {"title": title, "chat_type": chat.type.name}},
            )
        except Exception as _e_getchat:
            # Bot tidak bisa akses chat (sesi baru / bot lain) — periksa tipe tersimpan
            stored_type = (doc.get("chat_type") or "").upper()
            if stored_type == "CHANNEL":
                continue  # skip channel yang tersimpan di DB
            if not doc.get("title"):
                continue  # belum ada judul tersimpan, lewati

            # FIX Bug 1: Verifikasi apakah bot masih anggota grup.
            # Jika bot sudah dikeluarkan (UserNotParticipant / ChannelPrivate /
            # ChatForbidden), hapus data dari DB agar grup tidak muncul lagi.
            try:
                from pyrogram.errors import (
                    UserNotParticipant, ChannelPrivate, ChatForbidden,
                    ChatIdInvalid, PeerIdInvalid,
                )
                me = client.me
                await client.get_chat_member(chat_id, me.id)
                # Berhasil → bot masih di grup, lanjutkan
            except Exception as _ve:
                _err_cls = type(_ve).__name__
                if _err_cls in (
                    "UserNotParticipant", "ChannelPrivate", "ChatForbidden",
                    "ChatIdInvalid", "PeerIdInvalid",
                ):
                    # Bot sudah tidak ada di grup → bersihkan DB
                    await remove_group_data(chat_id)
                    continue
                # Error lain (jaringan, FloodWait, dsb.) → percayai data DB

        if chat_accessible:
            if await is_admin(client, chat_id, user_id):
                result.append({"id": chat_id, "title": title})
        else:
            # Bot tidak bisa akses sekarang — tetap verifikasi apakah user adalah admin
            if await is_admin(client, chat_id, user_id):
                result.append({"id": chat_id, "title": title})
    _admin_groups_cache[user_id] = (result, time.monotonic())
    return result


async def save_group_title(chat_id: int, title: str) -> None:
    await config_db.update_one(
        {"chat_id": chat_id},
        {"$set": {"chat_id": chat_id, "title": title}},
        upsert=True,
    )
    _config_cache.pop(chat_id, None)


async def save_group_username(chat_id: int, username: str | None) -> None:
    """
    Simpan/update username grup ke config_db agar bisa di-resolve via
    @username saat rewarm.

    Username None DITULIS juga (bukan diabaikan) — kalau grup berubah dari
    publik→privat, field lama harus ikut hilang, supaya panel "Grup
    Terdaftar" tidak menampilkan link t.me/username basi yang sudah tidak
    berlaku (lihat refresh_group_public_info).
    """
    await config_db.update_one(
        {"chat_id": chat_id},
        {"$set": {"chat_id": chat_id, "username": username}},
        upsert=True,
    )
    _config_cache.pop(chat_id, None)


async def save_group_invite_link(chat_id: int, invite_link: str | None) -> None:
    """Simpan/update invite link grup privat ke config_db."""
    await config_db.update_one(
        {"chat_id": chat_id},
        {"$set": {"chat_id": chat_id, "invite_link": invite_link}},
        upsert=True,
    )
    _config_cache.pop(chat_id, None)


async def refresh_group_public_info(client, chat_id: int) -> None:
    """
    Sinkronkan info akses publik grup (username ATAU invite link) ke
    config_db supaya tidak basi — dipanggil berkala oleh perm_watchdog
    untuk setiap grup yang masih aktif.

    Aturan:
      • Grup PUBLIK (punya @username) → simpan username terbaru, HAPUS
        invite_link tersimpan (tidak relevan lagi, grup sudah publik).
      • Grup PRIVAT (tidak ada @username) → simpan username=None, lalu
        coba buat 1 invite link via export_chat_invite_link().
          - Telegram me-reuse link primer yang masih aktif jika ada, dan
            hanya generate baru jika link lama sudah di-revoke/dihapus
            owner — jadi aman dipanggil berkala, TIDAK membuat link baru
            setiap siklus selama link lama masih hidup.
          - Jika bot tidak punya izin invite (ChatAdminRequired) → SKIP
            diam-diam, tidak melempar error, invite_link lama (jika ada)
            dibiarkan apa adanya (lebih baik link basi yang masih bisa
            dicoba daripada tidak ada sama sekali).

    Dipanggil dengan try/except oleh caller — fungsi ini sendiri tidak
    melempar exception ke pemanggil untuk error apapun selain bug internal.
    """
    from pyrogram.errors import ChatAdminRequired, UserNotParticipant, FloodWait, BadRequest

    try:
        chat = await client.get_chat(chat_id)
    except Exception:
        return  # grup tidak terakses sekarang — perm_watchdog yang urus status grup hilang

    username = getattr(chat, "username", None)
    await save_group_username(chat_id, username)
    await save_group_title(chat_id, chat.title or str(chat_id))

    if username:
        # Grup publik — invite link tidak relevan lagi, bersihkan sisa lama.
        await save_group_invite_link(chat_id, None)
        return

    # Grup privat — coba pastikan ada 1 invite link valid.
    try:
        link = await client.export_chat_invite_link(chat_id)
        if link:
            await save_group_invite_link(chat_id, link)
    except (ChatAdminRequired, UserNotParticipant):
        pass  # tidak ada izin undang — skip, jangan error
    except FloodWait:
        pass  # rate limit sesaat — coba lagi siklus watchdog berikutnya
    except BadRequest as e:
        # Beberapa tipe chat tidak mendukung invite link sama sekali, mis.
        # CHANNEL_MONOFORUM_UNSUPPORTED (channel dengan mode forum satu-arah)
        # — ini bukan masalah izin/rate-limit, dan tidak akan pernah berhasil
        # walau di-retry. Skip diam-diam supaya log tidak penuh noise untuk
        # kondisi yang memang tidak bisa diperbaiki.
        if "MONOFORUM" not in str(e).upper():
            print(f"[GroupInfo] export invite link ditolak grup {chat_id}: {e}")
    except Exception as e:
        print(f"[GroupInfo] Gagal export invite link grup {chat_id}: {e}")


async def remove_group_data(chat_id: int) -> None:
    await config_db.delete_one({"chat_id": chat_id})
    _config_cache.pop(chat_id, None)
    keys_to_remove = [k for k in _admin_cache if k[0] == chat_id]
    for k in keys_to_remove:
        _admin_cache.pop(k, None)
    print(f"[DB] Data grup {chat_id} dihapus (bot dikeluarkan).")


# ══════════════════════════════════════════════════════════════════════════════
# NEXUS DATABASE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

async def nexus_insert_kalimat(teks: str) -> bool:
    try:
        await nexus_kalimat_db.update_one(
            {"teks": teks},
            {
                "$setOnInsert": {
                    "teks":          teks,
                    "status_proses": 0,
                    "created_at":    datetime.now(TZ_WIB),
                }
            },
            upsert=True,
        )
        doc = await nexus_kalimat_db.find_one({"teks": teks})
        return doc is not None
    except Exception:
        return False


async def nexus_get_all_kalimat() -> list[str]:
    return [doc["teks"] async for doc in nexus_kalimat_db.find({})]


async def nexus_get_kalimat_count() -> tuple[int, int]:
    global _nexus_kalimat_count_cache
    now = time.monotonic()
    if _nexus_kalimat_count_cache and (now - _nexus_kalimat_count_cache[1]) < NEXUS_COUNT_TTL:
        return _nexus_kalimat_count_cache[0]
    total   = await nexus_kalimat_db.count_documents({})
    antrean = await nexus_kalimat_db.count_documents({"status_proses": 0})
    _nexus_kalimat_count_cache = ((total, antrean), now)
    return total, antrean


async def nexus_mark_all_processed():
    await nexus_kalimat_db.update_many({}, {"$set": {"status_proses": 1}})
    invalidate_nexus_counts()


async def nexus_delete_kalimat(teks: str) -> bool:
    result = await nexus_kalimat_db.delete_one({"teks": teks})
    if result.deleted_count > 0:
        invalidate_nexus_counts()
    return result.deleted_count > 0


async def nexus_delete_kalimat_by_id(id_str: str) -> bool:
    try:
        if _BACKEND == "mongo":
            from bson import ObjectId  # type: ignore
            result = await nexus_kalimat_db.delete_one({"_id": ObjectId(id_str)})
        else:
            result = await nexus_kalimat_db.delete_one({"_id": str(id_str)})
        if result.deleted_count > 0:
            invalidate_nexus_counts()
        return result.deleted_count > 0
    except Exception:
        return False


async def nexus_save_regex_bulk(pola_list: list[tuple[str, str]]):
    await nexus_regex_db.delete_many({})
    if pola_list:
        docs = [
            {
                "pola":       p,
                "kata_kunci": k,
                "created_at": datetime.now(TZ_WIB),
            }
            for p, k in pola_list
        ]
        await nexus_regex_db.insert_many(docs)
    invalidate_nexus_counts()


async def nexus_get_all_regex() -> list[dict]:
    return [
        {"pola": d["pola"], "kata_kunci": d["kata_kunci"]}
        async for d in nexus_regex_db.find({})
    ]


async def nexus_get_regex_count() -> int:
    global _nexus_regex_count_cache
    now = time.monotonic()
    if _nexus_regex_count_cache and (now - _nexus_regex_count_cache[1]) < NEXUS_COUNT_TTL:
        return _nexus_regex_count_cache[0]
    n = await nexus_regex_db.count_documents({})
    _nexus_regex_count_cache = (n, now)
    return n


async def nexus_delete_regex_by_pola(pola: str) -> bool:
    result = await nexus_regex_db.delete_one({"pola": pola})
    return result.deleted_count > 0


async def nexus_get_regex_page(page: int, limit: int = 5) -> tuple[list[dict], int]:
    total  = await nexus_regex_db.count_documents({})
    offset = (page - 1) * limit
    rows   = [
        {"pola": d["pola"], "kata_kunci": d["kata_kunci"]}
        async for d in nexus_regex_db.find({}).sort("_id", -1).skip(offset).limit(limit)
    ]
    return rows, total


async def nexus_get_kalimat_page(page: int, limit: int = 10) -> tuple[list[dict], int]:
    total  = await nexus_kalimat_db.count_documents({})
    offset = (page - 1) * limit
    rows   = [
        {
            "_id":           d["_id"],
            "teks":          d["teks"],
            "status_proses": d.get("status_proses", 0),
        }
        async for d in nexus_kalimat_db.find({}).sort("_id", -1).skip(offset).limit(limit)
    ]
    return rows, total


async def nexus_track_grup(chat_id: int, judul: str, username: str | None = None):
    set_fields = {"chat_id": chat_id, "judul": judul, "is_group": True}
    # Username grup publik (tanpa "@") — None/"" berarti grup privat, field
    # tetap ditulis supaya grup yang baru ganti dari publik→privat ikut
    # ter-update (tidak menyisakan username lama yang sudah tidak valid).
    set_fields["username"] = username or None
    await nexus_grup_db.update_one(
        {"chat_id": chat_id},
        {"$set": set_fields},
        upsert=True,
    )


async def nexus_remove_grup(chat_id: int):
    await nexus_grup_db.delete_one({"chat_id": chat_id})


async def nexus_get_all_grup() -> list[dict]:
    global _nexus_grup_cache
    now = time.monotonic()
    if _nexus_grup_cache and (now - _nexus_grup_cache[1]) < NEXUS_COUNT_TTL:
        return _nexus_grup_cache[0]
    result = [
        {
            "chat_id":  d["chat_id"],
            "judul":    d.get("judul", str(d["chat_id"])),
            "username": d.get("username"),
        }
        async for d in nexus_grup_db.find({"is_group": True})
    ]
    _nexus_grup_cache = (result, now)
    return result


async def nexus_clear_kalimat():
    await nexus_kalimat_db.delete_many({})
    await nexus_regex_db.delete_many({})
    invalidate_nexus_counts()


async def nexus_clear_regex():
    await nexus_regex_db.delete_many({})
    invalidate_nexus_counts()


# ══════════════════════════════════════════════════════════════════════════════
# NEXUS WHITELIST HELPERS
# ══════════════════════════════════════════════════════════════════════════════

async def nexus_whitelist_add(pola: str, raw: str, kata_list: list, mutasi: dict) -> bool:
    try:
        await nexus_whitelist_db.update_one(
            {"pola": pola},
            {
                "$set": {
                    "pola":       pola,
                    "raw":        raw,
                    "kata_list":  kata_list,
                    "mutasi":     mutasi,
                    "created_at": datetime.now(TZ_WIB),
                }
            },
            upsert=True,
        )
        invalidate_nexus_counts()
        return True
    except Exception:
        return False


async def nexus_whitelist_get_all() -> list[dict]:
    return [doc async for doc in nexus_whitelist_db.find({})]


async def nexus_whitelist_count() -> int:
    global _nexus_wl_count_cache
    now = time.monotonic()
    if _nexus_wl_count_cache and (now - _nexus_wl_count_cache[1]) < NEXUS_COUNT_TTL:
        return _nexus_wl_count_cache[0]
    n = await nexus_whitelist_db.count_documents({})
    _nexus_wl_count_cache = (n, now)
    return n


async def get_owner_regex_count() -> int:
    """Count Owner Regex (regex_db) dengan cache NEXUS_COUNT_TTL detik."""
    global _nexus_owner_regex_count_cache
    now = time.monotonic()
    if _nexus_owner_regex_count_cache and (now - _nexus_owner_regex_count_cache[1]) < NEXUS_COUNT_TTL:
        return _nexus_owner_regex_count_cache[0]
    n = await regex_db.count_documents({})
    _nexus_owner_regex_count_cache = (n, now)
    return n


async def nexus_regex_delete_by_id(object_id) -> bool:
    """Hapus satu pola dari regex_db (Owner Regex) berdasarkan _id dokumen."""
    try:
        if _BACKEND == "mongo":
            from bson import ObjectId  # type: ignore
            result = await regex_db.delete_one({"_id": ObjectId(str(object_id))})
        else:
            result = await regex_db.delete_one({"_id": str(object_id)})
        if result.deleted_count > 0:
            invalidate_nexus_counts()
        return result.deleted_count > 0
    except Exception:
        return False


async def nexus_whitelist_delete_by_id(object_id) -> bool:
    try:
        if _BACKEND == "mongo":
            from bson import ObjectId  # type: ignore
            result = await nexus_whitelist_db.delete_one({"_id": ObjectId(str(object_id))})
        else:
            result = await nexus_whitelist_db.delete_one({"_id": str(object_id)})
        if result.deleted_count > 0:
            invalidate_nexus_counts()
        return result.deleted_count > 0
    except Exception:
        return False


async def nexus_whitelist_page(page: int, limit: int = 5) -> tuple[list[dict], int]:
    total  = await nexus_whitelist_db.count_documents({})
    offset = (page - 1) * limit
    rows   = [
        doc
        async for doc in nexus_whitelist_db.find({}).sort("_id", -1).skip(offset).limit(limit)
    ]
    return rows, total


async def nexus_whitelist_clear() -> int:
    result = await nexus_whitelist_db.delete_many({})
    invalidate_nexus_counts()
    return result.deleted_count


# ══════════════════════════════════════════════════════════════════════════════
# NEXUS ACTION LOG HELPERS
# ══════════════════════════════════════════════════════════════════════════════
# Menyimpan riwayat tindakan bot (hapus, whitelist spared, keroyokan)
# agar bisa dipantau langsung dari panel bot tanpa buka LOG_CHANNEL.
# Maksimum 500 entri — entri terlama otomatis dihapus saat melewati batas.
# ══════════════════════════════════════════════════════════════════════════════

_ACTLOG_MAX = 500


async def nexus_actlog_insert(
    aksi:        str,    # "HAPUS" | "WHITELIST" | "KEROYOKAN"
    user_id:     int,
    user_name:   str,
    chat_id:     int,
    chat_title:  str,
    alasan:      str,    # kata kunci / layer AI
    confidence:  float,  # 0.0 jika bukan AI
    content:     str,    # cuplikan pesan (maks 200 char)
) -> None:
    try:
        doc = {
            "aksi":       aksi,
            "user_id":    user_id,
            "user_name":  user_name,
            "chat_id":    chat_id,
            "chat_title": chat_title,
            "alasan":     alasan[:200],
            "confidence": round(confidence, 4),
            "content":    content[:200],
            "ts":         datetime.now(TZ_WIB),
        }
        await nexus_actlog_db.insert_one(doc)
        # Pangkas jika melebihi batas
        total = await nexus_actlog_db.count_documents({})
        if total > _ACTLOG_MAX:
            # Hapus 50 entri terlama sekaligus
            oldest = [
                d["_id"]
                async for d in nexus_actlog_db.find({}).sort("_id", 1).limit(50)
            ]
            if oldest:
                await nexus_actlog_db.delete_many({"_id": {"$in": oldest}})
    except Exception as e:
        print(f"[DB] actlog_insert error (non-fatal): {e}")


async def nexus_actlog_get_page(page: int, limit: int = 5) -> tuple[list[dict], int]:
    total  = await nexus_actlog_db.count_documents({})
    offset = (page - 1) * limit
    rows   = [
        doc
        async for doc in nexus_actlog_db.find({}).sort("_id", -1).skip(offset).limit(limit)
    ]
    return rows, total


async def nexus_actlog_count() -> int:
    return await nexus_actlog_db.count_documents({})


async def nexus_actlog_clear() -> int:
    result = await nexus_actlog_db.delete_many({})
    return result.deleted_count


# ══════════════════════════════════════════════════════════════════════════════
# AI DEBUG LOG — 24h TTL
# Log internal aktivitas nexus/ai_core/ untuk dipantau owner.
# Data lama otomatis dihapus saat fungsi get_page dipanggil.
# ══════════════════════════════════════════════════════════════════════════════

_ai_debug_db = db["ai_debug_log"]


async def ai_debug_log_insert(
    aksi:       str,
    label:      str   = "-",
    confidence: float = 0.0,
    ringkasan:  str   = "",
    chat_id:    int   = 0,
) -> None:
    """Simpan satu entri log debug AI. Non-blocking, gagal diam-diam."""
    try:
        ts_now = int(datetime.now(timezone.utc).timestamp())
        doc = {
            "ts":         ts_now,
            "aksi":       aksi[:40],
            "label":      label[:16],
            "confidence": round(float(confidence), 4),
            "ringkasan":  ringkasan[:180],
            "chat_id":    int(chat_id),
        }
        await _ai_debug_db.insert_one(doc)
    except Exception as e:
        print(f"[DB] ai_debug_log_insert error: {e}")


async def ai_debug_log_get_page(
    page:     int = 1,
    per_page: int = 5,
) -> tuple[list[dict], int]:
    """
    Ambil halaman log debug AI (24 jam terakhir), urut terbaru dulu.
    Sekaligus membersihkan entri yang lebih dari 24 jam.
    Returns: (docs_halaman_ini, total_dalam_24j)
    """
    try:
        cutoff = int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp())

        # Load semua, lakukan filtering dan cleanup di Python
        # (aman untuk kedua backend SQLite & MongoDB)
        all_docs = await _ai_debug_db.find({}).to_list(None)

        # Pisahkan yang lama — hapus dari DB
        old_ids  = [d["_id"] for d in all_docs if d.get("ts", 0) < cutoff and "_id" in d]
        for oid in old_ids:
            try:
                await _ai_debug_db.delete_one({"_id": oid})
            except Exception:
                pass

        # Filter 24 jam terakhir, sort terbaru dulu
        recent = [d for d in all_docs if d.get("ts", 0) >= cutoff]
        recent.sort(key=lambda d: d.get("ts", 0), reverse=True)

        total = len(recent)
        start = (page - 1) * per_page
        return recent[start : start + per_page], total

    except Exception as e:
        print(f"[DB] ai_debug_log_get_page error: {e}")
        return [], 0


# ══════════════════════════════════════════════════════════════════════════════
# DM USER REGISTRY — untuk broadcast notifikasi shutdown/maintenance
# ══════════════════════════════════════════════════════════════════════════════

_dm_users_db = db["dm_users"]


async def register_dm_user(user_id: int) -> None:
    """Catat user yang pernah berinteraksi dengan bot via DM."""
    try:
        ts_now = int(datetime.now(timezone.utc).timestamp())
        await _dm_users_db.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id, "ts": ts_now}},
            upsert=True,
        )
    except Exception as e:
        print(f"[DB] register_dm_user error: {e}")


async def get_all_dm_users() -> list[int]:
    """Ambil semua user_id yang terdaftar untuk broadcast shutdown."""
    try:
        docs = await _dm_users_db.find({}).to_list(None)
        return [d["user_id"] for d in docs if isinstance(d.get("user_id"), int)]
    except Exception as e:
        print(f"[DB] get_all_dm_users error: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# LOCAL MUTE TRACKER — anti-duplikasi eskalasi hukuman
# ══════════════════════════════════════════════════════════════════════════════
# Schema per dokumen:
#   _id         : "lmute_{chat_id}_{user_id}"
#   chat_id     : int
#   user_id     : int
#   consec_spam : int    — hitungan duplikat berturut-turut (reset jika pesan bersih)
#   mute_level  : int    — level eskalasi; 0 = 5 mnt, 1 = 10 mnt, 2 = 20 mnt, dst.
#   muted_until : float  — unix timestamp akhir mute; 0.0 jika tidak sedang mute
#   updated_at  : float  — unix timestamp terakhir update
# ══════════════════════════════════════════════════════════════════════════════

local_mute_db = db["local_mute"]
warn_once_db  = db["warn_once"]   # Riwayat pemberitahuan — seumur hidup, tidak ada TTL
known_peers_db = db["known_peers"]  # Cache peer user: {uid, access_hash, first_name} — dibaca saat rewarm startup


# ── Known Peers — persist access_hash antar redeploy ─────────────────────────

async def upsert_known_peer(user_id: int, access_hash: int, first_name: str = "") -> None:
    """
    Simpan atau update data peer user ke MongoDB.
    Dipanggil setiap kali bot menerima pesan dari user di grup.

    Mengapa perlu ini:
      Pyrogram menyimpan access_hash di file .session (SQLite lokal).
      Setelah redeploy Railway, file .session di-restore dari MongoDB —
      tapi peer yang ditemui antara backup terakhir (20 mnt) dan redeploy hilang.
      Dengan menyimpan access_hash per-user ke MongoDB secara terpisah,
      saat startup kita bisa warm peer pakai InputUser(id, access_hash)
      yang PASTI berhasil — tidak bergantung pada session backup.
    """
    if get_active_backend() != "mongo":
        return
    try:
        await known_peers_db.update_one(
            {"_id": user_id},
            {"$set": {
                "access_hash": access_hash,
                "first_name":  first_name[:64] if first_name else "",
                "updated_at":  __import__("time").time(),
            }},
            upsert=True,
        )
    except Exception:
        pass  # Jangan crash bot hanya karena gagal simpan peer


async def get_all_known_peers() -> list[dict]:
    """
    Ambil semua peer yang pernah disimpan dari MongoDB.
    Return: list of {_id: user_id, access_hash: int, first_name: str}
    """
    if get_active_backend() != "mongo":
        return []
    try:
        return await known_peers_db.find(
            {}, {"_id": 1, "access_hash": 1, "first_name": 1}
        ).to_list(length=None)
    except Exception:
        return []


async def ensure_known_peers_index() -> None:
    """Pastikan index TTL dan updated_at ada — dipanggil sekali saat startup."""
    if get_active_backend() != "mongo":
        return
    try:
        # TTL 90 hari — peer yang lama tidak aktif otomatis terhapus
        await known_peers_db.create_index(
            "updated_at",
            expireAfterSeconds=90 * 24 * 3600,
            background=True,
        )
    except Exception:
        pass

_BASE_MUTE_SECONDS = 5 * 60   # 5 menit


def _mute_duration_seconds(mute_level: int) -> int:
    """Durasi mute dalam detik berdasarkan level eskalasi (2^level × 5 menit)."""
    return _BASE_MUTE_SECONDS * (2 ** mute_level)


async def get_local_mute(chat_id: int, user_id: int) -> dict:
    """Ambil atau buat rekaman mute untuk user di grup tertentu."""
    key = f"lmute_{chat_id}_{user_id}"
    doc = await local_mute_db.find_one({"_id": key})
    if doc is None:
        doc = {
            "_id":         key,
            "chat_id":     chat_id,
            "user_id":     user_id,
            "consec_spam": 0,
            "mute_level":  0,
            "muted_until": 0.0,
            "updated_at":  time.time(),
        }
    return doc


async def _save_local_mute(doc: dict) -> None:
    doc["updated_at"] = time.time()
    await local_mute_db.update_one(
        {"_id": doc["_id"]},
        {"$set": doc},
        upsert=True,
    )


async def increment_local_spam(chat_id: int, user_id: int) -> dict:
    """Tambah hitungan spam berturut-turut. Kembalikan dokumen terupdate."""
    doc = await get_local_mute(chat_id, user_id)
    doc["consec_spam"] = doc.get("consec_spam", 0) + 1
    await _save_local_mute(doc)
    return doc


async def apply_local_mute(chat_id: int, user_id: int) -> tuple[int, int]:
    """
    Terapkan mute berdasarkan level saat ini.
    Kembalikan (durasi_detik, level_yang_dipakai).
    Setelah dipanggil: consec_spam TIDAK direset ke 0 melainkan tetap di ambang
    (SPAM_MUTE_THRESHOLD) agar setelah mute habis, 1 pelanggaran berikutnya
    langsung memicu mute level berikutnya — hitungan punishment dilanjutkan,
    bukan dimulai dari awal. Restart bot tidak mempengaruhi ini karena
    consec_spam dan muted_until tersimpan persisten di database.

    PENTING: Fungsi ini menulis muted_until SEBELUM aksi mute API benar-benar
    dieksekusi (eksekusi terjadi async via core/moderation_queue.py). Jika
    eksekusi API gagal (bot bukan admin / kehilangan izin), pemanggil WAJIB
    memanggil revert_failed_local_mute(chat_id, user_id, level) — lihat
    fungsi tersebut — agar state "muted" tidak tertinggal palsu di DB.
    """
    _SPAM_MUTE_THRESHOLD = 10   # harus sama dengan SPAM_MUTE_THRESHOLD di punishment.py
    doc      = await get_local_mute(chat_id, user_id)
    level    = doc.get("mute_level", 0)
    duration = _mute_duration_seconds(level)
    doc["muted_until"] = time.time() + duration
    # Pertahankan consec_spam di ambang agar setelah mute habis langsung
    # mute lagi pada pelanggaran pertama (bukan harus 10x dari awal lagi).
    doc["consec_spam"] = _SPAM_MUTE_THRESHOLD
    doc["mute_level"]  = level + 1
    await _save_local_mute(doc)
    return duration, level


async def revert_failed_local_mute(chat_id: int, user_id: int, level_before: int) -> None:
    """
    Rollback state mute jika eksekusi API mute GAGAL (ChatAdminRequired,
    FloodWait lama yang di-skip, dsb) setelah apply_local_mute() sudah
    menulis muted_until ke DB.

    FIXED: Sebelumnya muted_until tetap tertinggal di DB walau mute API
    gagal — akibatnya pesan user berikutnya tetap dihapus otomatis (karena
    filter lain hanya cek muted_until di DB, bukan status mute asli di
    Telegram), padahal user tidak benar-benar dibatasi kirim pesan oleh
    Telegram. Sekarang muted_until direset ke 0 dan mute_level dikembalikan
    ke level sebelumnya, sehingga:
      - Pesan berikutnya TIDAK dihapus berdasarkan state mute palsu.
      - consec_spam tetap di ambang (SPAM_MUTE_THRESHOLD) — pelanggaran
        berikutnya tetap langsung memicu percobaan mute lagi.
      - mute_level tidak naik akibat percobaan yang gagal.
    """
    key = f"lmute_{chat_id}_{user_id}"
    doc = await local_mute_db.find_one({"_id": key})
    if doc is None:
        return
    doc["muted_until"] = 0.0
    doc["mute_level"]  = level_before
    await _save_local_mute(doc)


async def reset_local_mute(chat_id: int, user_id: int) -> None:
    """
    Reset hitungan dan level hukuman (dipanggil saat pesan bersih diterima).
    Jika belum ada rekaman, tidak melakukan apa-apa.
    """
    key = f"lmute_{chat_id}_{user_id}"
    doc = await local_mute_db.find_one({"_id": key})
    if doc is None:
        return
    # Hanya reset jika ada sesuatu yang perlu direset
    if doc.get("consec_spam", 0) == 0 and doc.get("mute_level", 0) == 0:
        return
    doc["consec_spam"] = 0
    doc["mute_level"]  = 0
    await _save_local_mute(doc)


# ══════════════════════════════════════════════════════════════════════════════
# WARN ONCE — pemberitahuan 1× seumur hidup per (user, grup, jenis_spam)
# ══════════════════════════════════════════════════════════════════════════════
# Dokumen: { "_id": "warn_{chat_id}_{user_id}_{warn_type}" }
# Tidak ada TTL/expiry — sekali tersimpan, berlaku selamanya.
#
# warn_type yang digunakan:
#   "dup"           — spam duplikat lokal (antispam.py)
#   "gcast"         — anti-gcast global (antispam.py)
#   "vc_bio"        — mute mic VC karena bio mengandung link
#   "vc_nonmember"  — mute mic VC karena bukan anggota grup
#   "vc_peer"       — mute mic VC karena peer belum dikenali
#   "typing"        — mute typing (jika ada filter typing di masa depan)
# ══════════════════════════════════════════════════════════════════════════════

async def has_warned_user(chat_id: int, user_id: int, warn_type: str) -> bool:
    """
    Kembalikan True jika user ini sudah PERNAH diberi pemberitahuan
    jenis warn_type di grup chat_id.
    Operasi read-only, sangat ringan (lookup by _id).
    """
    key = f"warn_{chat_id}_{user_id}_{warn_type}"
    doc = await warn_once_db.find_one({"_id": key})
    return doc is not None


async def mark_warned_user(chat_id: int, user_id: int, warn_type: str) -> None:
    """
    Tandai bahwa user ini sudah diberi pemberitahuan jenis warn_type
    di grup chat_id. Idempotent — aman dipanggil berkali-kali.
    """
    key = f"warn_{chat_id}_{user_id}_{warn_type}"
    await warn_once_db.update_one(
        {"_id": key},
        {"$set": {"_id": key, "chat_id": chat_id, "user_id": user_id,
                  "warn_type": warn_type, "ts": time.time()}},
        upsert=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# GROUP ACTION LOG — log aksi per grup (hapus/mute/ban), TTL 7 hari
# ══════════════════════════════════════════════════════════════════════════════

# FIX (performa lambat saat buka/geser menu Log Aktivitas):
#   Sebelumnya get_group_action_log_page menarik SEMUA dokumen grup itu dari DB
#   ke memori (`.find({"chat_id": chat_id}).to_list(None)`), lalu filter/​sort
#   7-hari dan cleanup expired dilakukan satu-per-satu di Python — termasuk
#   `delete_one` berulang di dalam loop tiap kali halaman dibuka/digeser.
#   Untuk grup yang sudah aktif berhari-hari, ini bisa berarti ribuan dokumen
#   ditarik & di-sort ulang hanya untuk menampilkan 10 baris per halaman,
#   sehingga next/prev terasa lambat.
#
#   Perbaikan:
#     1. Index compound (chat_id, ts) dibuat sekali saat startup (idempotent)
#        agar query & sort di MongoDB memakai index, bukan full scan.
#     2. Query halaman langsung pakai sort+skip+limit di level DB (lewat
#        AsyncCursor yang sudah mendukung ini di kedua backend), bukan narik
#        semua dokumen lalu slice di Python.
#     3. Total dihitung via count_documents (bukan len() dari semua dokumen).
#     4. Cleanup entri expired (>7 hari) sekarang satu panggilan delete_many,
#        bukan loop delete_one per dokumen.
_group_action_log_index_created = False

# Throttle cleanup expired — jangan delete_many di SETIAP render halaman,
# cukup sekali per grup per _CLEANUP_INTERVAL detik (cleanup tetap akurat
# karena query halaman selalu pakai filter ts > cutoff secara terpisah).
_group_action_log_last_cleanup: dict[int, float] = {}
_GROUP_ACTION_LOG_CLEANUP_INTERVAL = 600   # 10 menit


async def _ensure_group_action_log_index() -> None:
    """
    Buat index compound (chat_id, ts desc) pada group_action_log.
    Aman dipanggil berulang (idempotent) — no-op di SQLite.
    """
    global _group_action_log_index_created
    if _group_action_log_index_created:
        return
    try:
        from pymongo import ASCENDING, DESCENDING  # type: ignore
        await group_action_log_db.create_index(
            [("chat_id", ASCENDING), ("ts", DESCENDING)],
        )
        _group_action_log_index_created = True
    except Exception as e:
        print(f"[DB] Gagal buat index group_action_log: {e}")


async def insert_group_action_log(
    chat_id:   int,
    aksi:      str,   # "HAPUS" | "MUTE" | "BAN" | "UNADMIN" | "SECOS" | "MUTE-VC-MIC" | "UNMUTE-VC-MIC"
    alasan:    str,   # teks detail bebas (pola cocok, durasi, dll) — BUKAN label jenis
    user_id:   int,
    user_name: str,
    konten:    str = "",
    jenis:     str | None = None,   # kode VIOLATION_* dari core/violation_types.py
) -> None:
    """
    Simpan satu entri log aksi ke group_action_log. Non-blocking, gagal diam-diam.

    `jenis` (BARU): kode terstruktur (lihat core/violation_types.py) yang
    menentukan icon + label Indonesia seragam di panel log grup & LOG_CHANNEL.
    Opsional untuk backward-compat — entri tanpa `jenis` (atau ditulis sebelum
    field ini ada) tetap tampil lewat fallback generik di violation_types.py,
    tidak pernah error.
    """
    try:
        doc = {
            "chat_id":   chat_id,
            "ts":        time.time(),
            "aksi":      aksi[:20],
            "alasan":    alasan[:120],
            "user_id":   user_id,
            "user_name": user_name[:50],
            "konten":    konten[:100],
            "jenis":     jenis,
        }
        await group_action_log_db.insert_one(doc)
    except Exception as e:
        print(f"[DB] insert_group_action_log error (non-fatal): {e}")


async def get_group_action_log_page(
    chat_id:  int,
    page:     int = 1,
    per_page: int = 10,
) -> tuple[list[dict], int]:
    """
    Ambil halaman log aksi grup (7 hari terakhir), urut terbaru dulu.
    Sekaligus bersihkan entri > 7 hari.

    FIXED: tidak lagi menarik semua dokumen ke memori — sort/skip/limit
    dilakukan di level DB, dan cleanup expired pakai satu delete_many.
    """
    try:
        await _ensure_group_action_log_index()
        cutoff = time.time() - (7 * 86400)

        # Bersihkan entri expired — di-throttle per grup, bukan setiap render.
        # Query halaman tetap akurat karena selalu filter ts > cutoff secara
        # terpisah, jadi entri expired tidak akan pernah tampil meski belum
        # sempat dibersihkan dari DB.
        now_mono = time.monotonic()
        last_cleanup = _group_action_log_last_cleanup.get(chat_id, 0.0)
        if now_mono - last_cleanup >= _GROUP_ACTION_LOG_CLEANUP_INTERVAL:
            _group_action_log_last_cleanup[chat_id] = now_mono
            try:
                await group_action_log_db.delete_many(
                    {"chat_id": chat_id, "ts": {"$lt": cutoff}}
                )
            except Exception:
                pass

        query = {"chat_id": chat_id, "ts": {"$gt": cutoff}}
        total = await group_action_log_db.count_documents(query)

        start = (page - 1) * per_page
        docs  = await (
            group_action_log_db.find(query)
            .sort("ts", -1)
            .skip(start)
            .limit(per_page)
            .to_list(None)
        )
        return docs, total
    except Exception as e:
        print(f"[DB] get_group_action_log_page error: {e}")
        return [], 0


# ── NewsCore (Sistem Skor Keaktifan & Admin Otomatis) ──────────────────────────
newscore_stats_db  = db["newscore_stats"]   # skor chat per user per grup
newscore_admin_db  = db["newscore_admins"]  # riwayat admin aktif yang diangkat
newscore_cfg_db    = db["newscore_config"]  # konfigurasi newscore per grup
newscore_titled_db = db["newscore_titled_members"]  # member non-admin yang
                                             # sedang dipasang tag (Auto Title
                                             # Member) — dipakai untuk hapus tag
                                             # otomatis saat member itu tidak lagi
                                             # masuk daftar titel periode baru.
vip_titled_db      = db["vip_titled_members"]  # member VIP yang sedang dipasang
                                             # tag (Title VIP) — dipakai untuk
                                             # hapus tag otomatis saat status VIP
                                             # member itu hilang (lihat
                                             # core/vip_bio_guard.py & free.py).

# ── Cache ns_get_current_admins ───────────────────────────────────────────────
# ns_get_current_admins dipanggil setiap pesan dari NS admin (untuk cek apakah
# dia boleh dihitung skornya). Tanpa cache ini = query MongoDB per pesan.
# Cache TTL sama dengan ADMIN_TTL (120 detik) — konsisten dengan is_admin().
# Key: chat_id, Value: (list_admins, timestamp)
_ns_current_admins_cache: dict[int, tuple[list, float]] = {}
_NS_ADMINS_CACHE_TTL = ADMIN_TTL  # 120 detik — ikut ADMIN_TTL agar konsisten

# ── NewsCore score buffer (rate-limit aman) ───────────────────────────────────
# Daripada langsung update_one ke MongoDB per pesan (banyak grup ramai =
# ribuan write/menit), kita buffer skor di memory dulu lalu flush ke DB
# secara batch setiap _NS_FLUSH_INTERVAL detik.
# Buffer: {(chat_id, user_id): {"name": str, "delta": int}}
# Flush worker dijalankan dari antigcast.py setelah client.start().
import collections as _collections
_ns_score_buffer: dict[tuple, dict] = {}
_ns_score_buffer_lock = asyncio.Lock() if False else None  # diinisialisasi di ns_init_flush_worker
_NS_FLUSH_INTERVAL = int(os.environ.get("NS_FLUSH_INTERVAL", 10))  # detik


# ══════════════════════════════════════════════════════════════════════════════
# NEWSCORE — Sistem Skor Keaktifan & Admin Otomatis
# ══════════════════════════════════════════════════════════════════════════════

from datetime import datetime, timedelta as _timedelta

NEWSCORE_DEFAULT = {
    "enabled":        False,
    "mode":           "day",      # "day" | "date" | "weekday"
    "reset_days":     7,
    "reset_date":     1,
    "reset_weekday":  0,
    "reset_hour":     23,
    "reset_minute":   59,
    "max_admins":     1,
    "next_reset":     None,
    "bio_admin_text": "",   # Teks wajib di bio admin yang diangkat NewsCore.
    "bio_admin_required": True,  # False = sengaja dikosongkan via tombol "Kosongkan"
                             # → admin NewsCore TIDAK diwajibkan apapun di bio.
                             # True + bio_admin_text kosong (default awal, belum
                             # pernah diatur sama sekali) = dianggap wajib tapi
                             # mustahil dipenuhi (semua admin NewsCore di-unadmin
                             # sampai diisi ATAU sampai owner pilih "Kosongkan").
    "admin_title": "",      # Titel custom (maks 16 karakter) yang dipasang via
                             # set_administrator_title saat admin
                             # diangkat NewsCore tiap periode reset. Kosong =
                             # pakai titel default bawaan ("Top Member N 👑").
    "auto_title_enabled": False,  # Auto Title Member: tag otomatis (via Bot API
                             # setChatMemberTag) untuk member NON-admin berdasar
                             # rank typing/leaderboard NewsCore, dipasang bareng
                             # ns_do_reset(). Beda dari admin_title (itu khusus
                             # admin yang diangkat NewsCore).
    "auto_title_names": [],  # List hingga 10 nama tag, urut per kelompok rank
                             # 5 besar: index 0 -> rank 1-5, index 1 -> rank 6-10,
                             # dst. Kosong = fitur tidak aktif walau enabled=True.
    "privileges": {
        "can_delete_messages":   True,
        "can_restrict_members":  True,
        "can_invite_users":      True,
        "can_pin_messages":      True,
        "can_manage_video_chats": False,
    },
}

HARI_MAP_NS = {0: "Senin", 1: "Selasa", 2: "Rabu", 3: "Kamis",
               4: "Jumat", 5: "Sabtu", 6: "Minggu"}


async def ns_get_config(chat_id: int) -> dict:
    now = time.monotonic()
    hit = _ns_config_cache.get(chat_id)
    if hit and (now - hit[1]) < NS_CONFIG_TTL:
        return hit[0]
    doc = await newscore_cfg_db.find_one({"chat_id": chat_id})
    cfg = {k: v for k, v in NEWSCORE_DEFAULT.items()}
    cfg["privileges"] = dict(NEWSCORE_DEFAULT["privileges"])
    if doc:
        for k in NEWSCORE_DEFAULT:
            if k in doc:
                cfg[k] = doc[k]
        if "privileges" in doc:
            cfg["privileges"] = dict(doc["privileges"])
    _ns_config_cache[chat_id] = (cfg, now)
    return cfg


async def ns_update(chat_id: int, updates: dict) -> None:
    await newscore_cfg_db.update_one(
        {"chat_id": chat_id},
        {"$set": {"chat_id": chat_id, **updates}},
        upsert=True,
    )
    _ns_config_cache.pop(chat_id, None)  # invalidasi cache panel


def ns_update_optimistic(
    chat_id: int, updates: dict,
    dm_chat_id: int | None = None, dm_msg_id: int | None = None,
) -> dict:
    """
    Versi "ringan" dari ns_update — dipakai oleh tombol panel NewsCore DM.

    Cache di-update langsung (synchronous) supaya render ulang panel
    instan, sedangkan penulisan ke DB diantrikan via panel_write_queue
    dan dieksekusi belakangan oleh panel_write_worker. Jika gagal permanen
    dan dm_chat_id/dm_msg_id diisi, panel itu akan dikoreksi otomatis.

    Return dict config terbaru (hasil optimistic).
    """
    now = time.monotonic()
    hit = _ns_config_cache.get(chat_id)
    if hit:
        cfg = {k: v for k, v in hit[0].items()}
        cfg["privileges"] = dict(hit[0].get("privileges", NEWSCORE_DEFAULT["privileges"]))
    else:
        cfg = {k: v for k, v in NEWSCORE_DEFAULT.items()}
        cfg["privileges"] = dict(NEWSCORE_DEFAULT["privileges"])
    for k, v in updates.items():
        if k == "privileges":
            cfg["privileges"] = dict(v)
        else:
            cfg[k] = v
    _ns_config_cache[chat_id] = (cfg, now)
    enqueue_ns_write(chat_id, updates, dm_chat_id, dm_msg_id)
    return cfg


def ns_calc_next_reset(cfg: dict) -> str:
    # Pakai TZ_WIB eksplisit (bukan datetime.now() naive) agar next_reset
    # tidak meleset jika server hosting berjalan di timezone selain WIB
    # (mis. UTC default di Railway/Docker). Tanpa ini, jam yang dimasukkan
    # owner di UI (dimaksudkan WIB) bisa dieksekusi di jam yang berbeda.
    now   = datetime.now(TZ_WIB)
    h, m  = cfg.get("reset_hour", 23), cfg.get("reset_minute", 59)
    mode  = cfg.get("mode", "day")
    try:
        if mode == "day":
            days   = cfg.get("reset_days", 7)
            target = (now + _timedelta(days=days)).replace(hour=h, minute=m, second=0, microsecond=0)
        elif mode == "date":
            d = cfg.get("reset_date", 1)
            try:
                target = now.replace(day=d, hour=h, minute=m, second=0, microsecond=0)
                if target <= now:
                    raise ValueError
            except ValueError:
                if now.month == 12:
                    target = now.replace(year=now.year + 1, month=1, day=d, hour=h, minute=m, second=0, microsecond=0)
                else:
                    target = now.replace(month=now.month + 1, day=d, hour=h, minute=m, second=0, microsecond=0)
        else:
            wd         = cfg.get("reset_weekday", 0)
            days_ahead = wd - now.weekday()
            if days_ahead <= 0:
                days_ahead += 7
            target = (now + _timedelta(days=days_ahead)).replace(hour=h, minute=m, second=0, microsecond=0)
            if target <= now:
                target += _timedelta(days=7)
        return target.isoformat()
    except Exception:
        return (datetime.now(TZ_WIB) + _timedelta(days=7)).isoformat()


async def ns_track_message(chat_id: int, user_id: int, user_name: str) -> None:
    """
    Tambah 1 poin skor untuk user di grup.

    RATE-LIMIT SAFE: skor dibuffer di memory (_ns_score_buffer) dan di-flush
    ke MongoDB secara batch setiap _NS_FLUSH_INTERVAL detik oleh
    ns_flush_score_buffer(). Ini menghindari ribuan update_one per menit
    saat banyak grup ramai — DB hanya kena hit saat flush.
    """
    key = (chat_id, user_id)
    if key in _ns_score_buffer:
        _ns_score_buffer[key]["delta"] += 1
        _ns_score_buffer[key]["name"]   = user_name  # update nama terbaru
    else:
        _ns_score_buffer[key] = {"name": user_name, "delta": 1}


async def ns_flush_score_buffer() -> None:
    """
    Flush buffer skor ke MongoDB secara batch.
    Dipanggil oleh background worker (ns_flush_worker_loop) setiap
    _NS_FLUSH_INTERVAL detik. Juga dipanggil sebelum ns_reset_scores()
    agar tidak ada skor yang hilang saat reset tiba.
    """
    if not _ns_score_buffer:
        return

    # Ambil snapshot lalu bersihkan buffer (tidak ada asyncio.Lock di sini
    # karena single-threaded event loop — swap atomik cukup)
    snapshot = dict(_ns_score_buffer)
    _ns_score_buffer.clear()

    try:
        from pymongo import UpdateOne
        ops = [
            UpdateOne(
                {"chat_id": chat_id, "user_id": user_id},
                {"$set": {"user_name": data["name"]}, "$inc": {"score": data["delta"]}},
                upsert=True,
            )
            for (chat_id, user_id), data in snapshot.items()
        ]
        if ops:
            # Collection.bulk_write() (lihat database.py) — 1 round-trip per
            # shard yang relevan, BUKAN 1 round-trip per user seperti versi
            # lama yang salah asumsi newscore_stats_db punya atribut ._col
            # (selalu AttributeError → jatuh ke fallback update_one satu-satu,
            # meniadakan tujuan batching ini). newscore_stats_db sendiri bukan
            # collection yang di-shard, jadi ini selalu 1 round-trip total.
            await newscore_stats_db.bulk_write(ops, ordered=False)
    except Exception as e:
        print(f"[NewsCore] flush error: {e}")
        # Kembalikan data ke buffer agar tidak hilang
        for key, data in snapshot.items():
            if key in _ns_score_buffer:
                _ns_score_buffer[key]["delta"] += data["delta"]
            else:
                _ns_score_buffer[key] = data


async def ns_flush_worker_loop() -> None:
    """
    Background worker: flush score buffer ke DB setiap _NS_FLUSH_INTERVAL detik.
    Jalankan sekali dari antigcast.py setelah await app.start().
    """
    while True:
        await asyncio.sleep(_NS_FLUSH_INTERVAL)
        try:
            await ns_flush_score_buffer()
        except Exception as e:
            print(f"[NewsCore] flush_worker_loop error: {e}")


async def ns_get_leaderboard(chat_id: int, limit: int = 10) -> list:
    """
    Ambil leaderboard skor grup dari DB.
    Catatan: skor yang masih di buffer (_ns_score_buffer) belum termasuk.
    Untuk akurasi penuh saat digunakan pada reset, pastikan
    ns_flush_score_buffer() dipanggil terlebih dahulu (lihat ns_do_reset).
    """
    try:
        cur = newscore_stats_db.find({"chat_id": chat_id}).sort("score", -1).limit(limit)
        return await cur.to_list(length=limit)
    except Exception:
        return []


async def ns_get_active_user_count(chat_id: int) -> int:
    """
    Hitung total user aktif (pernah kirim pesan) dalam periode ini.
    Termasuk yang masih di buffer belum di-flush.
    """
    try:
        db_count = await newscore_stats_db.count_documents({"chat_id": chat_id})
        # Tambahkan user di buffer yang belum ada di DB
        buf_users = {uid for (cid, uid) in _ns_score_buffer if cid == chat_id}
        return db_count + len(buf_users)
    except Exception:
        return 0


async def ns_reset_scores(chat_id: int) -> None:
    """
    Reset semua skor di grup. Flush buffer dulu agar tidak ada skor hilang
    yang masih di memory saat reset dipanggil.
    """
    try:
        # Flush buffer → pastikan semua skor masuk DB sebelum dihapus
        await ns_flush_score_buffer()
        await newscore_stats_db.delete_many({"chat_id": chat_id})
    except Exception as e:
        print(f"[NewsCore] reset error: {e}")


async def ns_get_current_admins(chat_id: int) -> list:
    """
    Ambil daftar admin NewsCore aktif di grup.
    Di-cache _NS_ADMINS_CACHE_TTL detik (default 120s, sama dengan ADMIN_TTL)
    untuk menghindari query MongoDB per pesan saat NS admin sering chat.
    """
    now = time.monotonic()
    hit = _ns_current_admins_cache.get(chat_id)
    if hit and (now - hit[1]) < _NS_ADMINS_CACHE_TTL:
        return hit[0]
    try:
        result = await newscore_admin_db.find({"chat_id": chat_id}).to_list(length=20)
    except Exception:
        result = []
    _ns_current_admins_cache[chat_id] = (result, now)
    return result


async def ns_is_current_admin(chat_id: int, user_id: int) -> bool:
    """
    True jika user_id adalah admin NewsCore AKTIF di grup ini saat ini.

    Dipakai untuk MELARANG admin NewsCore menjadi VIP (baik manual /vip
    maupun otomatis via teks bio) — supaya admin NewsCore tidak pernah bisa
    lolos dari pengecekan "Bio Admin Wajib" hanya karena kebetulan (atau
    disengaja) bio-nya juga memenuhi teks VIP Bio grup yang sama.
    """
    try:
        admins = await ns_get_current_admins(chat_id)
        return user_id in {a["user_id"] for a in admins}
    except Exception:
        return False


def invalidate_ns_admins_cache(chat_id: int) -> None:
    """Hapus cache ns_get_current_admins untuk grup tertentu.
    Dipanggil setelah ns_set_current_admins() atau ns_remove_admin()
    agar data selalu fresh setelah ada perubahan daftar admin.
    """
    _ns_current_admins_cache.pop(chat_id, None)


async def ns_set_current_admins(chat_id: int, admins: list) -> None:
    try:
        await newscore_admin_db.delete_many({"chat_id": chat_id})
        if admins:
            await newscore_admin_db.insert_many(admins)
        invalidate_ns_admins_cache(chat_id)
    except Exception as e:
        print(f"[NewsCore] set admins error: {e}")


async def ns_remove_admin(chat_id: int, user_id: int) -> None:
    """Hapus satu admin NewsCore dari daftar admin aktif (tanpa menyentuh admin lain)."""
    try:
        await newscore_admin_db.delete_many({"chat_id": chat_id, "user_id": user_id})
        invalidate_ns_admins_cache(chat_id)
    except Exception as e:
        print(f"[NewsCore] remove admin error: {e}")


async def ns_get_titled_members(chat_id: int) -> list:
    """
    Ambil daftar member non-admin yang SEDANG dipasang tag (Auto Title
    Member) di grup ini dari periode reset sebelumnya.

    Dipakai _apply_auto_title_member() untuk membandingkan dengan daftar
    titel baru — siapa yang TIDAK lagi masuk daftar baru akan dihapus
    tag-nya (setChatMemberTag dengan tag="").
    """
    try:
        return await newscore_titled_db.find({"chat_id": chat_id}).to_list(length=200)
    except Exception as e:
        print(f"[NewsCore] get titled members error: {e}")
        return []


async def ns_set_titled_members(chat_id: int, members: list) -> None:
    """
    Timpa daftar member bertitel grup ini dengan daftar baru.
    `members` adalah list of dict {"chat_id", "user_id", "user_name", "tag"}.
    Dipanggil SETELAH semua setChatMemberTag (pasang baru + hapus lama)
    selesai dieksekusi di akhir periode reset.
    """
    try:
        await newscore_titled_db.delete_many({"chat_id": chat_id})
        if members:
            await newscore_titled_db.insert_many(members)
    except Exception as e:
        print(f"[NewsCore] set titled members error: {e}")


async def ns_remove_score(chat_id: int, user_id: int) -> None:
    """
    Hapus data skor user dari newscore_stats.
    Dipanggil saat member di-adminkan paksa (bukan via NewsCore) agar
    bot tidak mencoba meng-adminkan dia lagi di periode berikutnya.
    Juga bersihkan dari buffer in-memory jika belum di-flush.
    """
    try:
        # Hapus dari buffer in-memory (belum tentu ada di DB)
        _ns_score_buffer.pop((chat_id, user_id), None)
        # Hapus dari DB
        await newscore_stats_db.delete_many({"chat_id": chat_id, "user_id": user_id})
    except Exception as e:
        print(f"[NewsCore] remove_score error: {e}")


# ── Title VIP — daftar member VIP yang sedang dipasang tag ────────────────────
async def vip_get_titled_members(chat_id: int) -> list:
    """
    Ambil daftar member VIP yang SEDANG dipasang tag (Title VIP) di grup ini.

    Dipakai untuk membandingkan dengan daftar VIP terbaru — siapa yang
    TIDAK lagi VIP akan dihapus tag-nya (setChatMemberTag dengan tag="").
    """
    try:
        return await vip_titled_db.find({"chat_id": chat_id}).to_list(length=500)
    except Exception as e:
        print(f"[VIP-Title] get titled members error: {e}")
        return []


async def vip_set_titled_member(chat_id: int, user_id: int, tag: str) -> None:
    """Catat/timpa satu member VIP sebagai sedang bertitel tag tertentu."""
    try:
        await vip_titled_db.update_one(
            {"chat_id": chat_id, "user_id": user_id},
            {"$set": {"chat_id": chat_id, "user_id": user_id, "tag": tag}},
            upsert=True,
        )
    except Exception as e:
        print(f"[VIP-Title] set titled member error: {e}")


async def vip_remove_titled_member(chat_id: int, user_id: int) -> None:
    """Hapus satu member dari daftar bertitel VIP (setelah tag-nya dicopot)."""
    try:
        await vip_titled_db.delete_one({"chat_id": chat_id, "user_id": user_id})
    except Exception as e:
        print(f"[VIP-Title] remove titled member error: {e}")


async def get_vip_user_ids(chat_id: int) -> set:
    """
    Ambil seluruh user_id Member VIP aktif di grup ini (manual /vip ATAU
    bio_vip — keduanya disimpan di free_per_group, lihat core/vip_bio_guard.py
    & plugins/commands/free.py).

    Dipakai untuk MENGECUALIKAN member VIP dari Auto Title Member NewsCore
    (plugins/commands/newscore.py — sama seperti admin NS dikecualikan),
    supaya kuota 5 member per tier tetap terisi penuh oleh member non-VIP.
    """
    try:
        docs = await db["free_per_group"].find({"chat_id": chat_id}, {"user_id": 1}).to_list(length=2000)
        return {d["user_id"] for d in docs if "user_id" in d}
    except Exception as e:
        print(f"[VIP-Title] get_vip_user_ids error: {e}")
        return set()



# ══════════════════════════════════════════════════════════════════════════════
# MENTION MEMBER CACHE
# ══════════════════════════════════════════════════════════════════════════════
#
# Menyimpan hasil get_chat_member per (chat_id, user_id/username) agar
# deteksi external mention tidak perlu hit Telegram API setiap saat.
#
# Schema dokumen:
#   chat_id    : int          — ID grup
#   user_id    : int          — ID user (tidak pernah berubah)
#   username   : str | None   — @username saat di-cache (bisa berubah)
#   is_member  : bool         — True = member grup ini
#   cached_at  : float        — unix timestamp saat data disimpan
#   expires_at : datetime     — TTL 1 minggu, diperbarui tiap ada mention
#
# Index:
#   unik  (chat_id, user_id)   — lookup by user_id
#   biasa (chat_id, username)  — lookup by username string
#   TTL   expires_at           — MongoDB hapus otomatis setelah 1 minggu
#
# username dan user_id keduanya disimpan karena berbeda:
#   - user_id stabil → cocok untuk TEXT_MENTION / tg://user?id=
#   - username bisa berubah → disimpan apa adanya saat di-cache
#   Jika user ganti username, entry lama (username lama) tetap ada sampai
#   TTL habis. Entry baru (username baru) dibuat saat mention berikutnya.
#   Ini aman karena worst case hanya false negative (miss cache → API call),
#   bukan false positive (member asli dikira external).

MENTION_CACHE_TTL_SECS      = 30 * 24 * 3600   # 1 bulan — per-grup member cache
MENTION_GLOBAL_NON_AKUN_TTL = 30 * 24 * 3600   # 30 hari — username tidak exist / akun mati
MENTION_GLOBAL_ENTITY_TTL   =  7 * 24 * 3600   # 7 hari  — channel / grup

_mention_ttl_index_created = False


async def ensure_mention_cache_index() -> None:
    """
    Buat TTL index pada expires_at di mention_member_cache.
    Idempotent — aman dipanggil tiap startup.
    """
    global _mention_ttl_index_created
    if _mention_ttl_index_created:
        return
    if _BACKEND != "mongo":
        _mention_ttl_index_created = True
        return
    try:
        await mention_cache_db.create_index("expires_at", expireAfterSeconds=0)
        await mention_cache_db.create_index([("chat_id", 1), ("user_id", 1)], unique=True)
        await mention_cache_db.create_index([("chat_id", 1), ("username", 1)])
        _mention_ttl_index_created = True
        print("[MentionCache] ✅ TTL index mention_member_cache siap.")
    except Exception as e:
        print(f"[MentionCache] ⚠️  Gagal buat index: {e}")


def _mention_expires_at() -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=MENTION_CACHE_TTL_SECS)


async def mention_cache_get_by_username(chat_id: int, username: str) -> "bool | None":
    """
    Cari di cache berdasarkan username (lowercase, tanpa @).
    Return: True = member, False = bukan member, None = tidak ada di cache.
    """
    try:
        doc = await mention_cache_db.find_one({
            "chat_id": chat_id,
            "username": username.lower(),
        })
        if doc is None:
            return None
        return doc.get("is_member", False)
    except Exception as e:
        print(f"[MentionCache] get_by_username error: {e}")
        return None


async def mention_cache_get_by_uid(chat_id: int, user_id: int) -> "bool | None":
    """
    Cari di cache berdasarkan user_id.
    Return: True = member, False = bukan member, None = tidak ada di cache.
    """
    try:
        doc = await mention_cache_db.find_one({
            "chat_id": chat_id,
            "user_id": user_id,
        })
        if doc is None:
            return None
        return doc.get("is_member", False)
    except Exception as e:
        print(f"[MentionCache] get_by_uid error: {e}")
        return None


async def mention_cache_set(
    chat_id: int,
    user_id: int,
    is_member: bool,
    username: "str | None" = None,
) -> None:
    """
    Simpan / perbarui cache hasil member check.
    TTL diperbarui ke 1 minggu dari sekarang setiap dipanggil.
    username dan user_id keduanya disimpan (bisa berbeda dari waktu ke waktu).
    """
    try:
        now = time.time()
        await mention_cache_db.update_one(
            {"chat_id": chat_id, "user_id": user_id},
            {"$set": {
                "chat_id":    chat_id,
                "user_id":    user_id,
                "username":   username.lower() if username else None,
                "is_member":  is_member,
                "cached_at":  now,
                "expires_at": _mention_expires_at(),
            }},
            upsert=True,
        )
        # Jika punya username, update juga entry by username
        # (untuk kasus user_id belum dikenal tapi username dikenal)
        if username:
            await mention_cache_db.update_one(
                {"chat_id": chat_id, "username": username.lower(), "user_id": {"$ne": user_id}},
                {"$set": {
                    "chat_id":    chat_id,
                    "user_id":    user_id,
                    "username":   username.lower(),
                    "is_member":  is_member,
                    "cached_at":  now,
                    "expires_at": _mention_expires_at(),
                }},
                upsert=False,   # hanya update jika ada dokumen username lama yang user_id-nya beda
            )
    except Exception as e:
        print(f"[MentionCache] set error: {e}")


async def mention_cache_refresh_ttl(chat_id: int, user_id: "int | None" = None, username: "str | None" = None) -> None:
    """
    Perbarui expires_at ke MENTION_CACHE_TTL_SECS lagi tanpa mengubah data lain.
    Dipanggil setiap ada mention dan entry sudah ada di cache (cache hit) —
    baik member MAUPUN bukan member, supaya keduanya tetap "awet" selama
    masih aktif disebut, tidak cuma yang member.

    Bisa dipanggil dengan user_id (paling akurat — entry unik per user_id)
    ATAU username saja (untuk mention @username yang belum di-resolve ke
    user_id, mis. dari bot utama yang belum sempat lewat monitor bot).
    Minimal salah satu harus diisi.
    """
    if user_id is None and not username:
        return
    try:
        query: dict = {"chat_id": chat_id}
        if user_id is not None:
            query["user_id"] = user_id
        else:
            query["username"] = username.lower()
        await mention_cache_db.update_one(
            query,
            {"$set": {"expires_at": _mention_expires_at()}},
        )
    except Exception as e:
        print(f"[MentionCache] refresh_ttl error: {e}")


async def mention_cache_remove_member(chat_id: int, user_id: int) -> None:
    """
    Tandai user sebagai bukan member lagi (misal: keluar/kick dari grup).
    Tidak menghapus dokumen — TTL tetap jalan, tapi is_member = False.
    """
    try:
        await mention_cache_db.update_one(
            {"chat_id": chat_id, "user_id": user_id},
            {"$set": {
                "is_member":  False,
                "cached_at":  time.time(),
                "expires_at": _mention_expires_at(),
            }},
        )
    except Exception as e:
        print(f"[MentionCache] remove_member error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
#  MENTION GLOBAL CACHE — non-akun, channel, grup (lintas semua grup)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Koleksi ini menyimpan 3 jenis entri:
#   kind="non_akun"  — username tidak exist / akun mati / bukan entitas valid
#   kind="channel"   — username milik channel Telegram
#   kind="grup"      — username milik grup / supergroup Telegram
#
# Setelah tersimpan, cek per-grup (API call) tidak perlu diulang untuk username
# yang sama di grup manapun — langsung keputusan dari sini.
#
# TTL:
#   non_akun → 30 hari (username mati lama tidak aktif)
#   channel/grup → 7 hari (bisa berubah pemilik atau jenis)
# ───────────────────────────────────────────────────────────────────────────────

_mention_global_index_created = False


async def ensure_mention_global_index() -> None:
    """Index TTL dan username unik untuk mention_global_cache. Idempotent."""
    global _mention_global_index_created
    if _mention_global_index_created:
        return
    if _BACKEND != "mongo":
        _mention_global_index_created = True
        return
    try:
        await mention_global_db.create_index("expires_at", expireAfterSeconds=0)
        await mention_global_db.create_index("username", unique=True)
        _mention_global_index_created = True
        print("[MentionGlobal] ✅ Index mention_global_cache siap.")
    except Exception as e:
        print(f"[MentionGlobal] ⚠️  Gagal buat index: {e}")


async def mention_global_get(username: str) -> "dict | None":
    """
    Cari username di global cache.
    Return doc {"kind": "non_akun"|"channel"|"grup", ...} atau None jika tidak ada.
    """
    try:
        return await mention_global_db.find_one({"username": username.lower()})
    except Exception:
        return None


async def mention_global_set(username: str, kind: str) -> None:
    """
    Simpan username ke global cache.
    kind: "non_akun" | "channel" | "grup"
    TTL disesuaikan per kind.
    """
    if kind == "non_akun":
        ttl_secs = MENTION_GLOBAL_NON_AKUN_TTL
    else:
        ttl_secs = MENTION_GLOBAL_ENTITY_TTL

    expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_secs)
    try:
        await mention_global_db.update_one(
            {"username": username.lower()},
            {"$set": {
                "username":   username.lower(),
                "kind":       kind,
                "cached_at":  time.time(),
                "expires_at": expires_at,
            }},
            upsert=True,
        )
    except Exception as e:
        print(f"[MentionGlobal] set error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
#  MENTION WHITELIST — per grup, username raw
# ═══════════════════════════════════════════════════════════════════════════════

async def mention_wl_get(chat_id: int) -> list[str]:
    """Ambil semua username whitelist untuk grup ini (lowercase, tanpa @)."""
    try:
        doc = await mention_wl_db.find_one({"chat_id": chat_id})
        return doc.get("usernames", []) if doc else []
    except Exception:
        return []


async def mention_wl_add(chat_id: int, username: str) -> bool:
    """
    Tambah username ke whitelist grup.
    Return True jika berhasil ditambah, False jika sudah ada.
    """
    uname = username.lower().lstrip("@")
    try:
        existing = await mention_wl_db.find_one({"chat_id": chat_id})
        if existing and uname in existing.get("usernames", []):
            return False
        await mention_wl_db.update_one(
            {"chat_id": chat_id},
            {"$addToSet": {"usernames": uname}},
            upsert=True,
        )
        return True
    except Exception as e:
        print(f"[MentionWL] add error: {e}")
        return False


async def mention_wl_remove(chat_id: int, username: str) -> bool:
    """
    Hapus username dari whitelist grup.
    Return True jika berhasil dihapus, False jika tidak ada.
    """
    uname = username.lower().lstrip("@")
    try:
        existing = await mention_wl_db.find_one({"chat_id": chat_id})
        if not existing or uname not in existing.get("usernames", []):
            return False
        await mention_wl_db.update_one(
            {"chat_id": chat_id},
            {"$pull": {"usernames": uname}},
        )
        return True
    except Exception as e:
        print(f"[MentionWL] remove error: {e}")
        return False
