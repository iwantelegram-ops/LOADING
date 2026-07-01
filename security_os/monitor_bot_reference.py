"""
monitor_bot_reference.py
════════════════════════════════════════════════════════════════════════════════
MANAJER BOT PEMANTAU — Security OS (Multi-Instance, Database-Driven)

ARSITEKTUR:
  Tiap grup Security OS punya 1 bot pemantau sendiri (token berbeda).
  Token disimpan di DB: security_os.monitor_token (diisi saat admin setup).
  File ini menjalankan SEMUA bot pemantau dalam SATU proses — tiap bot
  berjalan sebagai Pyrogram Client tersendiri (instance terpisah).

  ┌────────────────────────────────────────────────────────────────────┐
  │  monitor_bot_reference.py (proses ini)                             │
  │                                                                    │
  │   MonitorInstance(chat_id=grupA, token=tokenA)  ← pantau grupA    │
  │   MonitorInstance(chat_id=grupB, token=tokenB)  ← pantau grupB    │
  │   MonitorInstance(chat_id=grupC, token=tokenC)  ← pantau grupC    │
  │                                                                    │
  │   Semua tulis ke collection bio_profiles dengan field chat_id     │
  └──────────────────────────────┬─────────────────────────────────────┘
                                 │ DB bersama (MongoDB)
              ┌──────────────────┴──────────────────┐
              ▼                                     ▼
    ┌──────────────────┐                 ┌──────────────────────┐
    │   Bot Utama      │  query          │      Userbot         │
    │  bio_filter      │  bio_profiles   │   (VC kick)          │
    │  (chat_id=grupA) │  {user_id,      │   (chat_id=grupA)    │
    └──────────────────┘   chat_id}      └──────────────────────┘

COLLECTION bio_profiles:
  {
    chat_id    : int,    # ID grup (tiap grup data terpisah)
    user_id    : int,    # ID user
    has_link   : bool,   # True = ada link di bio
    bio        : str,    # isi bio saat dicek
    checked_at : float,  # unix timestamp terakhir dicek
    updated_at : float,  # unix timestamp terakhir berubah status
    expires_at : datetime, # TTL — dokumen otomatis dihapus MongoDB setelah N detik
  }
  Index unik: (chat_id, user_id)
  Index TTL : expires_at (expireAfterSeconds=0) → MongoDB hapus otomatis

FLOW TOKEN:
  1. Admin aktifkan Security OS di grup → bot utama minta token bot pemantau
  2. Admin kirim token via DM ke bot utama
  3. Bot utama validasi token → simpan ke security_os.monitor_token
  4. Bot utama panggil reload_monitor_instances() (fungsi di file ini)
  5. File ini spawn MonitorInstance baru untuk token/grup tersebut

STRATEGI CACHE (upgrade anti-FloodWait):
  Semua fetch GetFullUser HANYA dilakukan oleh bot pemantau via _bio_worker
  (antrian rate-limit, 1 request per _BIO_QUEUE_DELAY detik per instance).

  Trigger TYPING tidak lagi langsung fetch — hanya catat user ke
  _recent_active. Background worker _cache_fill_worker mengisi cache
  secara pelan (BIO_FILL_DELAY_SECS per user, default 3 detik) sehingga
  burst typing dari banyak user tidak menyebabkan burst GetFullUser.

  TTL di-jitter ±BIO_TTL_JITTER_SECS per instance agar expires antar grup
  tidak barengan dan menimbulkan lonjakan fetch bersamaan.

  Fetch langsung ke API (via _enqueue_bio_check) hanya terjadi untuk:
    1. User JOIN grup (force=True, sekali per join)
    2. User NAIK VC  (force=True, throttle VC_JOIN_RECHECK_SECS)
    3. User KIRIM PESAN pertama kali / cache sudah expired (force=False
       tapi _cache_fill_worker yang enqueue, bukan handler pesan)
    4. User perubahan profil terdeteksi (UpdateUserName/Photo)
    5. force_check_user dari bio.py: HANYA jika cache benar-benar kosong
       (None dari DB), lewat antrian yang sama — tidak bypass queue

VARIABEL .env:
  API_ID, API_HASH   — sama dengan bot utama
  MONGO_URL          — HARUS SAMA dengan bot utama (DB bersama)
  MONGO_DB_NAME      — HARUS SAMA dengan bot utama
  CODE_BOT           — HARUS SAMA dengan bot utama
  BIO_TTL_SECS       — TTL data bio di DB (default: 300 detik / 5 menit)
  BIO_RECHECK_SECS   — throttle minimum antar re-fetch (default = BIO_TTL_SECS)
  VC_JOIN_RECHECK_SECS — throttle re-fetch saat naik VC (default = BIO_TTL_SECS)
  BIO_QUEUE_DELAY    — jeda antar GetFullUser dalam worker (default: 1.5 detik)
  BIO_FILL_DELAY_SECS — jeda antar fetch di background fill loop (default: 3 detik)
  BIO_TTL_JITTER_SECS — random ±N detik pada TTL per instance (default: 60 detik)
  BIO_ACTIVE_WINDOW_SECS — window "user aktif" untuk fill loop (default: 2×TTL)
"""

from __future__ import annotations

import os
import re
import time
import random
import asyncio
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.types import Message, ChatMemberUpdated
from pyrogram.raw import functions as raw_fns, types as raw_types
from pyrogram.errors import FloodWait, PeerIdInvalid, ChatAdminRequired

# FIX (pemindahan ke folder security_os/): file ini sekarang berada di
# <root>/security_os/monitor_bot_reference.py. .env tetap di ROOT proyek,
# jadi pakai parent.parent (bukan parent saja).
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env", override=False)

# ── Env ───────────────────────────────────────────────────────────────────────
API_ID   = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")

# TTL data bio di MongoDB — dokumen dihapus otomatis setelah N detik (default 300)
BIO_TTL_SECS = int(os.environ.get("BIO_TTL_SECS", 300))

# Throttle minimum antar re-fetch — default mengikuti BIO_TTL_SECS.
# Ubah BIO_TTL_SECS saja sudah cukup untuk mengatur seluruh lapisan cache.
BIO_RECHECK_SECS     = int(os.environ.get("BIO_RECHECK_SECS", BIO_TTL_SECS))
VC_JOIN_RECHECK_SECS = int(os.environ.get("VC_JOIN_RECHECK_SECS", BIO_TTL_SECS))

# ── Rate-limit & fill-loop config ─────────────────────────────────────────────
# Jeda antar GetFullUser dalam _bio_worker (per instance).
# 1.5 detik → maks ~40 req/menit per token, aman untuk grup ramai.
# Naikkan jika masih kena FloodWait, turunkan jika mau lebih responsif.
_BIO_QUEUE_DELAY = float(os.environ.get("BIO_QUEUE_DELAY", 1.5))

# Timeout (detik) _enqueue_bio_check menunggu HASIL satu kali fetch bio.
# Ini BUKAN soal "berapa lama tunggu sampai nyerah karena macet/FloodWait" —
# ini soal "berapa lama tunggu satu fetch yang NORMAL selesai". Fetch normal
# (tidak FloodWait) biasanya selesai dalam BIO_QUEUE_DELAY s/d ~1.5 detik.
# Default 4 detik: cukup longgar untuk fetch normal + 1-2 antrian di depan,
# tapi tidak bikin grup nge-lag lama kalau memang macet/FloodWait/Bengkel
# belum sempat masuk. JANGAN diset 0 — itu bikin SEMUA fetch (termasuk yang
# normal & cepat) selalu dianggap "belum selesai" walau sebenarnya tinggal
# sepersekian detik lagi, sehingga bio-link-detector & VIP-bio-text efektif
# nonaktif untuk pesan pertama siapapun, bukan cuma saat macet.
_ENQUEUE_TIMEOUT = float(os.environ.get("BIO_ENQUEUE_TIMEOUT_SECS", 4.0))

# Jeda antar fetch di _cache_fill_worker (background loop per instance).
# 3 detik default — lebih lambat dari _bio_worker karena tidak mendesak.
_BIO_FILL_DELAY = float(os.environ.get("BIO_FILL_DELAY_SECS", 3.0))

# Jitter TTL: expires_at = sekarang + BIO_TTL_SECS ± random(0, JITTER)
# Tiap instance punya offset jitter berbeda agar TTL antar grup tidak barengan.
_BIO_TTL_JITTER = int(os.environ.get("BIO_TTL_JITTER_SECS", 60))

# Window "user dianggap aktif" untuk fill loop: default 2× TTL.
_BIO_ACTIVE_WINDOW = int(os.environ.get("BIO_ACTIVE_WINDOW_SECS", BIO_TTL_SECS * 2))

# ── Pola deteksi link di bio ──────────────────────────────────────────────────
LINK_PATTERN = re.compile(
    r"(@\S+|https?://\S+|t\.me/\S+|bit\.ly/\S+|linktr\.ee/\S+)",
    re.IGNORECASE,
)

TZ_WIB = timezone(timedelta(hours=7))

# ── Database — pakai modul yang sama dengan bot utama ────────────────────────
# FIX (pemindahan ke folder security_os/): tambahkan ROOT proyek ke sys.path
# (bukan folder security_os/ tempat file ini berada) — database.py tetap di
# root. Ini membuat modul ini tetap bisa diimpor mandiri (tidak rapuh
# terhadap urutan import dari antigcast.py), persis seperti sebelum
# pemindahan.
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database import (  # noqa: E402
    db, _init_backend, get_bot_config, save_bot_config, get_active_backend,
    mention_cache_set, mention_cache_get_by_uid, mention_cache_get_by_username,
    mention_cache_refresh_ttl, mention_cache_remove_member,
)

bio_col = db["bio_profiles"]   # Collection hasil scan — dibaca bot utama & userbot
sec_col = db["security_os"]    # Untuk ambil daftar grup + token

# ── Registry instance aktif ───────────────────────────────────────────────────
_active_instances: dict[int, "MonitorInstance"] = {}
_instances_lock = asyncio.Lock()

# ── Flag: TTL index sudah dibuat ──────────────────────────────────────────────
_ttl_index_created = False


async def _ensure_ttl_index() -> None:
    """
    Buat TTL index pada field expires_at di bio_profiles.
    MongoDB akan otomatis hapus dokumen saat expires_at sudah lewat.
    Dipanggil sekali saat startup — aman dipanggil berulang (idempotent).
    """
    global _ttl_index_created
    if _ttl_index_created:
        return
    try:
        await bio_col.create_index(
            "expires_at",
            expireAfterSeconds=0,
        )
        print("[Monitor] ✅ TTL index bio_profiles.expires_at siap.")
        _ttl_index_created = True
    except Exception as e:
        print(f"[Monitor] ⚠️  Gagal buat TTL index: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# DETEKSI TOKEN MATI PERMANEN
# ══════════════════════════════════════════════════════════════════════════════

def _is_monitor_token_dead_error(exc: BaseException) -> bool:
    """
    True jika exception ini menandakan token bot pemantau sudah TIDAK VALID
    LAGI SECARA PERMANEN — bukan error sementara (network/FloodWait).

    Kasus permanen:
      - SESSION_REVOKED  : owner regenerate/revoke token via @BotFather
                           (/revoke), token lama langsung mati selamanya.
      - USER_DEACTIVATED : akun bot itu sendiri sudah dihapus/dinonaktifkan
                           (owner hapus bot dari @BotFather, atau Telegram
                           menonaktifkannya).
      - AUTH_KEY_UNREGISTERED / AuthKeyInvalid : variasi lain dari sesi yang
                           sudah dicabut total.

    Tanpa deteksi ini, MonitorInstance.start() akan terus dicoba ulang setiap
    kali bot utama restart — token yang sudah mati permanen TIDAK AKAN PERNAH
    berhasil lagi, jadi mencoba ulang hanya membanjiri log tanpa gunanya.

    Dicek lewat nama class (konsisten dengan _is_session_dead_error di
    video_call.py) supaya tetap terdeteksi walau ada perbedaan exception
    hierarchy antar versi pyrogram/pyrofork.
    """
    name = type(exc).__name__
    if name in (
        "SessionRevoked",
        "UserDeactivated",
        "UserDeactivatedBan",
        "AuthKeyUnregistered",
        "AuthKeyInvalid",
    ):
        return True
    msg = str(exc)
    return any(code in msg for code in (
        "SESSION_REVOKED", "USER_DEACTIVATED", "AUTH_KEY_UNREGISTERED",
    ))


async def _disable_dead_monitor(chat_id: int, reason: str) -> None:
    """
    Dipanggil saat token bot pemantau grup ini terkonfirmasi mati permanen.

    Menghapus monitor_token & monitor_bot_id dari dokumen security_os grup
    ini (TIDAK menghapus seluruh dokumen — enabled & setting lain tetap
    ada), supaya:
      1. _load_instances_from_db() tidak lagi mencoba spawn instance ini
         di restart berikutnya (filter query mensyaratkan monitor_token
         tidak kosong).
      2. check_activation_prerequisites di video_call.py otomatis mendeteksi
         "bot pemantau belum dikonfigurasi" lagi → admin diminta pasang
         token baru lewat panel, alur yang sudah ada.

    Tidak fatal jika gagal — kalau update DB gagal, instance ini akan
    dicoba lagi di restart berikutnya (sama seperti sebelumnya, tidak
    lebih buruk).
    """
    try:
        await sec_col.update_one(
            {"chat_id": chat_id},
            {"$set": {"monitor_token": "", "monitor_bot_id": 0}},
        )
        print(
            f"[Monitor {chat_id}] 🛑 Token mati permanen ({reason}) — "
            f"monitor_token dihapus dari DB agar tidak dicoba lagi. "
            f"Admin grup perlu pasang token bot pemantau baru."
        )
    except Exception as e:
        print(f"[Monitor {chat_id}] Gagal menghapus token mati dari DB: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# KELAS UTAMA — SATU INSTANCE PER GRUP
# ══════════════════════════════════════════════════════════════════════════════

class MonitorInstance:
    """
    Satu bot pemantau untuk satu grup.
    Punya Pyrogram Client sendiri (token unik per grup).
    Bereaksi terhadap event: pesan masuk, user join, typing, perubahan profil.

    ANTI-FLOODWAIT:
      Semua GetFullUser melewati _bio_worker (antrian, 1 req per _BIO_QUEUE_DELAY).
      Typing tidak langsung fetch — hanya catat ke _recent_active.
      _cache_fill_worker mengisi cache pelan di background untuk user aktif.
      TTL di-jitter per instance agar expire antar grup tidak barengan.
    """

    def __init__(self, chat_id: int, token: str, bot_id: int):
        self.chat_id  = chat_id
        self.token    = token
        self.bot_id   = bot_id
        self._stopped = False

        # Timestamp terakhir fetch berhasil per user (in-memory throttle)
        self._last_checked: dict[int, float] = {}
        self._last_vc_checked: dict[int, float] = {}

        # User yang baru-baru ini aktif (typing/kirim pesan) → target fill loop
        # user_id → last_seen timestamp
        self._recent_active: dict[int, float] = {}

        # Jitter offset unik per instance (deterministik dari chat_id)
        # Rentang: 0 s/d _BIO_TTL_JITTER detik — tiap grup punya offset beda
        self._ttl_jitter_offset: int = abs(chat_id) % max(_BIO_TTL_JITTER, 1)

        session_name = f"monitor_{abs(chat_id)}"
        # FIX (pemindahan ke folder security_os/): file .session tetap di ROOT
        # proyek (parent.parent), bukan di dalam security_os/, agar lokasi file
        # session monitor tidak berubah dibanding sebelum pemindahan.
        self._session_path   = str(Path(__file__).resolve().parent.parent / session_name) + ".session"
        self._session_db_key = f"monitor_session_{abs(chat_id)}"
        self.client = Client(
            session_name,
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=token,
        )

        self._raw_handler_registered = False

        # ── Bio-check queue: antrian agar GetFullUser tidak burst ─────────────
        # Item: (user_id, asyncio.Future) — worker proses 1 per 1 + jeda
        self._bio_queue: asyncio.Queue = asyncio.Queue()
        self._bio_queue_pending: set[int] = set()   # dedup: sedang antri/diproses
        self._bio_worker_task: Optional[asyncio.Task] = None

        # ── Background cache fill worker ──────────────────────────────────────
        self._fill_worker_task: Optional[asyncio.Task] = None

        # ── State FloodWait & "freeze" (dipinjam bot Bengkel) ──────────────────
        # busy_until: monotonic timestamp; instance dianggap FloodWait sampai
        # waktu ini terlewati. Diset oleh _bio_worker saat _fetch_bio gagal
        # dengan FloodWait, dibaca oleh workshop_join_pool untuk memutuskan
        # kapan grup ini perlu dibantu Bengkel.
        self.busy_until: float = 0.0
        # frozen: True selagi grup ini dalam mode kerja gabungan (≥1 Bengkel
        # confirmed joined). Saat frozen, _bio_worker instance ASLI berhenti
        # memproses queue sendiri — _GroupWorkRotation yang konsumsi queue &
        # _recent_active ini secara bergantian (round-robin, termasuk giliran
        # pemantau sendiri lewat self.client), supaya tidak ada 2 consumer
        # yang sama-sama proses 1 queue bersamaan.
        self.frozen: bool = False

    def is_floodwait(self, now: float | None = None) -> bool:
        """True jika instance ini sedang dianggap FloodWait (busy_until belum lewat)."""
        return (now if now is not None else time.monotonic()) < self.busy_until

    def mark_floodwait(self, seconds: float) -> None:
        """
        Tandai instance ini FloodWait selama `seconds` detik ke depan.
        Dipanggil dari _bio_worker setiap kali _fetch_bio menangkap FloodWait
        di langkah manapun (lihat _fetch_bio) — bukan cuma dicatat lokal,
        tapi disimpan di instance supaya workshop_join_pool bisa membaca
        status grup ini dari luar tanpa perlu akses ke detail fetch.
        """
        self.busy_until = max(self.busy_until, time.monotonic() + seconds)

    # ── TTL helper (jitter per instance) ─────────────────────────────────────

    def _make_expires_at(self) -> datetime:
        """
        Return datetime UTC kapan dokumen bio harus dihapus.
        Nilai = sekarang + BIO_TTL_SECS + jitter_offset
        Jitter deterministik per instance (dari chat_id) — bukan random setiap
        call — sehingga TTL antar grup konsisten tapi tidak barengan.
        """
        return datetime.now(timezone.utc) + timedelta(
            seconds=BIO_TTL_SECS + self._ttl_jitter_offset
        )

    # ── Session management ────────────────────────────────────────────────────

    async def _restore_session(self) -> None:
        """
        Pulihkan file .session monitor ini dari MongoDB jika file lokal belum ada
        (misal setelah Railway redeploy — filesystem container selalu bersih).
        Tanpa ini, tiap monitor selalu login dari nol dan peer cache-nya kosong
        setiap kali container di-restart.
        """
        import base64

        if get_active_backend() != "mongo":
            return
        if os.path.exists(self._session_path):
            return  # file lokal sudah ada, tidak perlu restore
        try:
            saved = await get_bot_config(self._session_db_key)
            if not saved:
                return
            raw = base64.b64decode(saved.encode())
            with open(self._session_path, "wb") as f:
                f.write(raw)
            print(f"[Monitor {self.chat_id}] ✅ Session dipulihkan dari MongoDB.")
        except Exception as e:
            print(f"[Monitor {self.chat_id}] ⚠️  Gagal pulihkan session: {e}")

    async def _save_session(self) -> None:
        """
        Backup file .session monitor ini (termasuk peer cache di dalamnya) ke
        MongoDB. Dipanggil setelah start() berhasil dan saat stop() — sehingga
        peer baru yang ditemui monitor selama berjalan ikut terbawa ke redeploy
        berikutnya.
        """
        import base64

        if get_active_backend() != "mongo":
            return
        try:
            if not os.path.exists(self._session_path):
                return
            with open(self._session_path, "rb") as f:
                raw = f.read()
            encoded = base64.b64encode(raw).decode()
            await save_bot_config(self._session_db_key, encoded)
        except Exception as e:
            print(f"[Monitor {self.chat_id}] ⚠️  Gagal simpan session: {e}")

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> bool:
        try:
            await self._restore_session()
            await self.client.start()
            await self._save_session()
            self._register_handlers()
            # Worker antrian fetch (rate-limit aman)
            self._bio_worker_task = asyncio.create_task(
                self._bio_worker(), name=f"bio_worker_{abs(self.chat_id)}"
            )
            # Background loop pengisi cache pelan untuk user aktif
            self._fill_worker_task = asyncio.create_task(
                self._cache_fill_worker(), name=f"fill_worker_{abs(self.chat_id)}"
            )
            print(
                f"[Monitor {self.chat_id}] ✅ Bot pemantau aktif "
                f"(queue_delay={_BIO_QUEUE_DELAY}s, fill_delay={_BIO_FILL_DELAY}s, "
                f"ttl_jitter=+{self._ttl_jitter_offset}s)."
            )
            return True
        except Exception as e:
            print(f"[Monitor {self.chat_id}] ❌ Gagal start: {e}")
            if _is_monitor_token_dead_error(e):
                # Token mati permanen — retry di restart berikutnya tidak
                # akan pernah berhasil. Hapus dari DB agar tidak dicoba lagi.
                await _disable_dead_monitor(self.chat_id, f"{type(e).__name__}: {e}")
            return False

    async def stop(self) -> None:
        self._stopped = True
        # Hentikan kedua worker
        for task in (self._bio_worker_task, self._fill_worker_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        # Simpan session terbaru sebelum client berhenti
        await self._save_session()
        try:
            if self.client.is_connected:
                await self.client.stop()
        except Exception:
            pass
        print(f"[Monitor {self.chat_id}] 🛑 Bot pemantau dihentikan.")

    # ── Fetch bio dari Telegram API ───────────────────────────────────────────

    async def _fetch_bio(self, user_id: int) -> str | None:
        """
        Ambil bio user via Telegram API. Return None jika gagal.

        Bekerja untuk SIAPAPUN yang ada di VC — termasuk user yang BUKAN member
        grup. Fallback 2 (get_chat_member) akan gagal untuk non-member tapi
        ditangkap dan dilanjutkan ke fallback 3 & 4.

        Strategi EMPAT langkah:
        1. resolve_peer(user_id) → GetFullUser  ← cepat
        2. get_chat_member(chat_id, user_id) → warm session → GetFullUser
           ← GAGAL untuk non-member (UserNotParticipant), dilanjutkan ke F3
        3. InputUser(user_id, access_hash=0) → GetFullUser
           ← bekerja untuk user publik
        4. get_users([user_id]) → resolve_peer → GetFullUser
           ← last resort, paksa Telegram kirim access_hash
        """
        async def _getfull(peer) -> str | None:
            try:
                full = await self.client.invoke(raw_fns.users.GetFullUser(id=peer))
                return getattr(full.full_user, "about", None) or ""
            except FloodWait:
                # JANGAN ditelan di sini — caller (langkah 1-4 di bawah) perlu
                # tahu ini FloodWait, bukan kegagalan biasa, supaya bisa
                # mark_floodwait() di level instance (dibaca workshop_join_pool).
                raise
            except Exception:
                return None

        # ── 1. Cepat: resolve_peer langsung ─────────────────────────────────
        try:
            peer = await self.client.resolve_peer(user_id)
            bio = await _getfull(peer)
            if bio is not None:
                return bio
        except FloodWait as fw:
            print(f"[Monitor {self.chat_id}] FloodWait {fw.value}s uid={user_id}")
            self.mark_floodwait(fw.value + 1)
            await asyncio.sleep(fw.value + 1)
            return None
        except (PeerIdInvalid, KeyError):
            pass
        except Exception as e:
            print(f"[Monitor {self.chat_id}] Gagal ambil bio uid={user_id}: {e}")
            return None

        # ── 2. Fallback: get_chat_member → warm session → resolve_peer ──────
        #    CATATAN: Gagal untuk non-member (UserNotParticipant) — ditangkap,
        #    lanjut ke fallback 3 & 4. Non-member di VC tetap diperiksa.
        try:
            member = await self.client.get_chat_member(self.chat_id, user_id)
            if member and member.user:
                try:
                    peer = await self.client.resolve_peer(member.user.id)
                    bio = await _getfull(peer)
                    if bio is not None:
                        print(f"[Monitor {self.chat_id}] uid={user_id}: bio via get_chat_member ✓")
                        return bio
                except FloodWait as fw_inner:
                    self.mark_floodwait(fw_inner.value + 1)
                    raise
                except Exception:
                    pass
        except FloodWait as fw2:
            print(f"[Monitor {self.chat_id}] FloodWait (fallback2) {fw2.value}s uid={user_id}")
            self.mark_floodwait(fw2.value + 1)
            await asyncio.sleep(fw2.value + 1)
            return None
        except Exception as e2:
            # UserNotParticipant = non-member; error lain = peer tak terjangkau
            # Tetap lanjut ke fallback 3 & 4 — jangan berhenti di sini
            print(
                f"[Monitor {self.chat_id}] Fallback2 gagal uid={user_id} "
                f"({type(e2).__name__}) — lanjut fallback 3+4"
            )

        # ── 3. Fallback: InputUser(access_hash=0) ───────────────────────────
        try:
            from pyrogram.raw.types import InputUser as _RawInputUser
            peer = _RawInputUser(user_id=user_id, access_hash=0)
            bio = await _getfull(peer)
            if bio is not None:
                print(f"[Monitor {self.chat_id}] uid={user_id}: bio via access_hash=0 ✓")
                return bio
        except FloodWait as fw3:
            self.mark_floodwait(fw3.value + 1)
            await asyncio.sleep(fw3.value + 1)
            return None
        except Exception:
            pass

        # ── 4. Fallback: get_users → paksa Pyrogram fetch access_hash ────────
        try:
            users_result = await self.client.get_users(user_id)
            u = users_result[0] if isinstance(users_result, list) else users_result
            if u:
                peer = await self.client.resolve_peer(u.id)
                bio = await _getfull(peer)
                if bio is not None:
                    print(f"[Monitor {self.chat_id}] uid={user_id}: bio via get_users ✓")
                    return bio
        except FloodWait as fw4:
            print(f"[Monitor {self.chat_id}] FloodWait (fallback4) {fw4.value}s uid={user_id}")
            self.mark_floodwait(fw4.value + 1)
            await asyncio.sleep(fw4.value + 1)
        except Exception:
            pass

        print(f"[Monitor {self.chat_id}] ⚠️  Semua fallback gagal uid={user_id} — bio tidak tersedia")
        return None

    # ── Bio-check queue worker ────────────────────────────────────────────────

    async def _bio_worker(self) -> None:
        """
        Worker tunggal per-instance — konsumsi _bio_queue satu per satu.
        Jeda _BIO_QUEUE_DELAY detik antar request untuk menghindari FloodWait
        saat banyak grup ramai dan banyak user masuk antrian bersamaan.

        Ini adalah SATU-SATUNYA tempat GetFullUser dipanggil — KECUALI saat
        instance ini di-freeze (self.frozen=True), di mana _GroupWorkRotation
        (core/workshop_join_pool.py) yang mengambil alih konsumsi _bio_queue &
        _recent_active instance ini selama mode kerja gabungan. Worker INI
        (_bio_worker) cuma idle-poll tanpa konsumsi apapun selagi frozen —
        "giliran pemantau" dalam rotasi tetap dilayani, tapi lewat
        koordinator rotasi (pakai self.client yang sama), bukan loop ini,
        supaya tidak ada 2 consumer yang bersaing ambil dari queue yang sama.
        """
        while not self._stopped:
            if self.frozen:
                # Sedang mode kerja gabungan — giliran pemantau dilayani lewat
                # _GroupWorkRotation (workshop_join_pool.py), bukan loop ini.
                # Jangan konsumsi queue di sini, cek lagi sebentar.
                await asyncio.sleep(1.0)
                continue
            try:
                user_id, future = await asyncio.wait_for(
                    self._bio_queue.get(), timeout=5.0
                )
            except asyncio.TimeoutError:
                continue
            except Exception:
                continue

            try:
                result = await self._check_and_save_impl(user_id)
                if not future.done():
                    future.set_result(result)
            except Exception as e:
                if not future.done():
                    try:
                        future.set_exception(e)
                    except Exception:
                        pass
            finally:
                self._bio_queue_pending.discard(user_id)
                self._bio_queue.task_done()
                if not self._stopped:
                    await asyncio.sleep(_BIO_QUEUE_DELAY)

    def drain_queue(self) -> int:
        """
        Kosongkan _bio_queue instance ini SEKARANG — batalkan semua future
        yang masih menunggu (caller-nya, mis. force_check_user yang sedang
        di-await, akan langsung dapat None alih-alih timeout 30 detik).

        Dipanggil oleh workshop_join_pool TEPAT SEBELUM freeze, saat instance
        ini terdeteksi FloodWait dan Bengkel akan mengambil alih. User yang
        memang masih perlu dicek akan otomatis di-enqueue ULANG oleh Bengkel
        sendiri (lewat _recent_active yang masih ada / request baru yang
        datang selagi Bengkel aktif) — bukan tugas drain_queue ini.

        Return: jumlah item yang dibuang (untuk logging).
        """
        drained = 0
        while True:
            try:
                user_id, future = self._bio_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._bio_queue_pending.discard(user_id)
            if not future.done():
                try:
                    future.set_result(None)
                except Exception:
                    pass
            self._bio_queue.task_done()
            drained += 1
        return drained

    async def _enqueue_bio_check(self, user_id: int) -> "bool | None":
        """
        Masukkan user_id ke antrian bio-check dan tunggu hasilnya, maksimal
        _ENQUEUE_TIMEOUT detik.

        Kenapa 4 detik (bukan 30, bukan 0):
          - Fetch NORMAL (tidak FloodWait, tidak macet) cuma butuh sekitar
            _BIO_QUEUE_DELAY s/d ~1.5 detik dari saat masuk antrian sampai
            hasil siap — 4 detik cukup longgar untuk itu (termasuk kalau ada
            1-2 antrian lain di depannya), jadi mayoritas pemanggil TETAP
            dapat hasil fetch yang sebenarnya, bukan langsung nyerah.
          - Kalau memang macet/FloodWait/queue menumpuk (mis. instance
            sedang frozen menunggu Bengkel, atau Bengkel sendiri kena
            FloodWait), 4 detik tetap jauh lebih singkat dari 30 detik lama
            — grup tidak nge-lag lama menunggu satu pesan diputuskan.

        Dipanggil oleh:
          - check_and_save(force=True)  — join grup, VC, perubahan profil
          - _cache_fill_worker          — pengisian cache background
          - force_check_user (via bio.py) — hanya saat cache kosong

        CATATAN BENGKEL: kalau instance sedang frozen, _bio_worker tidak
        konsumsi queue ini — _GroupWorkRotation (core/workshop_join_pool.py)
        yang konsumsi queue yang sama secara round-robin (giliran pemantau +
        giliran tiap Bengkel yang confirmed joined). Kalau belum ada Bengkel
        confirmed joined sama sekali (masih di _pending_groups / masih proses
        verifikasi join), queue ini akan timeout di sini setelah 4 detik —
        bio.py punya fallback ke workshop_pool standalone setelahnya (dan
        is_pending_for_bengkel() membuat bio.py malah skip menunggu di sini
        sama sekali untuk kasus itu, lihat plugins/filters/bio.py).
        """
        if user_id in self._bio_queue_pending:
            # Sudah antri — kembalikan data DB sementara (tidak menambah antrian)
            doc = await bio_col.find_one({"chat_id": self.chat_id, "user_id": user_id})
            return doc.get("has_link", False) if doc else None

        loop = asyncio.get_event_loop()
        future: asyncio.Future = loop.create_future()
        self._bio_queue_pending.add(user_id)
        await self._bio_queue.put((user_id, future))
        try:
            return await asyncio.wait_for(asyncio.shield(future), timeout=_ENQUEUE_TIMEOUT)
        except asyncio.TimeoutError:
            print(f"[Monitor {self.chat_id}] _enqueue timeout ({_ENQUEUE_TIMEOUT}s) uid={user_id}")
            return None
        except Exception as e:
            print(f"[Monitor {self.chat_id}] _enqueue error uid={user_id}: {e}")
            return None

    async def _check_and_save_impl(self, user_id: int) -> "bool | None":
        """
        Implementasi inti — hanya dipanggil oleh _bio_worker.
        Sudah dijamin tidak burst karena dijalankan satu per satu oleh worker.
        Menggunakan _make_expires_at() dengan jitter per instance.
        """
        now      = time.time()
        bio_text = await self._fetch_bio(user_id)
        if bio_text is None:
            return None

        has_link = bool(LINK_PATTERN.search(bio_text))
        self._last_checked[user_id] = now

        try:
            from core.ns_bio_guard import check_admin_bio_text
            admin_bio_ok = await check_admin_bio_text(self.chat_id, user_id, bio_text)
        except Exception as e:
            print(f"[Monitor {self.chat_id}] gagal cek admin_bio_ok uid={user_id}: {e}")
            admin_bio_ok = None

        old_doc      = await bio_col.find_one({"chat_id": self.chat_id, "user_id": user_id})
        old_has_link = old_doc.get("has_link") if old_doc else None
        updated_at   = (
            now
            if old_has_link != has_link
            else (old_doc.get("updated_at", now) if old_doc else now)
        )
        expires_at = self._make_expires_at()   # jitter per instance

        await bio_col.update_one(
            {"chat_id": self.chat_id, "user_id": user_id},
            {"$set": {
                "chat_id":      self.chat_id,
                "user_id":      user_id,
                "has_link":     has_link,
                "bio":          bio_text[:500],
                "checked_at":   now,
                "updated_at":   updated_at,
                "expires_at":   expires_at,
                "admin_bio_ok": admin_bio_ok,
            }},
            upsert=True,
        )

        if old_has_link != has_link:
            status = "ADA LINK" if has_link else "HAPUS LINK"
            print(f"[Monitor {self.chat_id}] uid={user_id} → {status} | bio: {bio_text[:80]!r}")

        return has_link

    # ── Background cache fill worker ──────────────────────────────────────────

    async def _cache_fill_worker(self) -> None:
        """
        Background loop: isi/refresh cache bio untuk user yang baru-baru ini aktif.

        TUJUAN: Menggantikan peran fetch langsung dari trigger typing.
          Typing (dan kirim pesan) hanya mencatat user ke _recent_active.
          Loop ini yang memutuskan kapan fetch dilakukan — secara pelan,
          satu per satu, dengan jeda _BIO_FILL_DELAY antar user.

        LOGIKA PRIORITAS per iterasi:
          1. Kumpulkan user aktif dalam _BIO_ACTIVE_WINDOW detik terakhir
          2. Skip user yang cache-nya masih fresh (>30% TTL tersisa di DB)
          3. Skip user yang sedang antri di _bio_worker (dedup via _bio_queue_pending)
          4. Enqueue user sisanya — _bio_worker yang jalankan dengan rate-limit

        Dengan _BIO_FILL_DELAY = 3 detik:
          - 10 user aktif = 30 detik untuk 1 siklus penuh
          - Tidak mungkin burst — jauh di bawah limit Telegram
        """
        while not self._stopped:
            try:
                if self.frozen:
                    # Mode kerja gabungan aktif — jangan tambah beban ke
                    # _bio_queue dari sisi ini. _GroupWorkRotation yang akan
                    # mengisi cache user aktif selama freeze (baca
                    # _recent_active milik instance ini secara bergantian,
                    # termasuk lewat giliran pemantau sendiri).
                    await asyncio.sleep(2.0)
                    continue

                now        = time.time()
                candidates = [
                    uid for uid, last_seen in list(self._recent_active.items())
                    if now - last_seen < _BIO_ACTIVE_WINDOW
                ]

                for user_id in candidates:
                    if self._stopped:
                        break

                    # Skip jika sedang antri di _bio_worker (dedup)
                    if user_id in self._bio_queue_pending:
                        continue

                    # Cek apakah cache masih fresh di DB
                    try:
                        doc = await bio_col.find_one(
                            {"chat_id": self.chat_id, "user_id": user_id}
                        )
                    except Exception:
                        doc = None

                    if doc:
                        expires = doc.get("expires_at")
                        if expires:
                            # Konversi ke timestamp — support naive & aware datetime
                            if hasattr(expires, "timestamp"):
                                expires_ts = expires.timestamp()
                            else:
                                expires_ts = 0
                            remaining = expires_ts - time.time()
                            ttl_total = BIO_TTL_SECS + self._ttl_jitter_offset
                            # Cache masih fresh jika sisa TTL > 30% total TTL
                            if remaining > ttl_total * 0.3:
                                continue

                    # Cache kosong atau akan segera expire → masukkan antrian
                    # Tidak await hasilnya — fire-and-forget via _bio_worker
                    if user_id not in self._bio_queue_pending:
                        loop = asyncio.get_event_loop()
                        future: asyncio.Future = loop.create_future()
                        self._bio_queue_pending.add(user_id)
                        await self._bio_queue.put((user_id, future))
                        # Buang future — kita tidak perlu hasilnya di sini
                        future.add_done_callback(lambda _: None)

                    # Jeda antar user agar fill loop tidak burst
                    await asyncio.sleep(_BIO_FILL_DELAY)

                # Bersihkan _recent_active yang sudah di luar window (hemat memori)
                cutoff = time.time() - _BIO_ACTIVE_WINDOW
                stale_uids = [uid for uid, ts in self._recent_active.items() if ts < cutoff]
                for uid in stale_uids:
                    self._recent_active.pop(uid, None)

                # Tidur sebelum iterasi berikutnya
                await asyncio.sleep(max(_BIO_FILL_DELAY, 5.0))

            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[Monitor {self.chat_id}] _cache_fill_worker error: {e}")
                await asyncio.sleep(10.0)

    # ── Public check methods ──────────────────────────────────────────────────

    async def check_and_save(
        self, user_id: int, force: bool = False
    ) -> "bool | None":
        """
        Cek bio user, simpan ke bio_profiles dengan chat_id grup ini.

        Return: True (ada link) | False (tidak) | None (gagal fetch)

        force=False → baca cache DB jika dalam throttle BIO_RECHECK_SECS.
        force=True  → enqueue fetch baru via _bio_worker (rate-limit aman).

        RATE-LIMIT SAFE: semua request ke Telegram API lewat _bio_worker
        (antrian per-instance, jeda _BIO_QUEUE_DELAY). Tidak burst.
        """
        now = time.time()

        if not force:
            last = self._last_checked.get(user_id, 0)
            if now - last < BIO_RECHECK_SECS:
                # Kembalikan data dari DB tanpa hit API
                doc = await bio_col.find_one(
                    {"chat_id": self.chat_id, "user_id": user_id}
                )
                return doc.get("has_link", False) if doc else None

        # Lewat antrian → _bio_worker proses satu per satu + jeda
        return await self._enqueue_bio_check(user_id)

    async def check_and_save_vc(self, user_id: int) -> bool | None:
        """
        Paksa re-check bio saat user NAIK KE VOICE CHAT.
        Cache khusus VC: VC_JOIN_RECHECK_SECS (default = BIO_TTL_SECS).

        BUG 2 FIX: Jika user tidak dikenal bot (tidak ada di DB) meski dalam
        throttle → bypass throttle, paksa fetch fresh dari Telegram API.
        Memastikan user yang baru pertama kali naik VC tetap di-scan.

        BUG 3 FIX (timestamp): Jika fetch gagal (result=None), timestamp
        tidak disimpan → scan berikutnya langsung retry tanpa tunggu TTL.
        """
        now      = time.time()
        last_vc  = self._last_vc_checked.get(user_id, 0)
        last_gen = self._last_checked.get(user_id, 0)
        last_any = max(last_vc, last_gen)

        if now - last_any < VC_JOIN_RECHECK_SECS:
            doc = await bio_col.find_one(
                {"chat_id": self.chat_id, "user_id": user_id}
            )
            if doc is not None:
                # Data fresh ada di DB → pakai langsung
                return doc.get("has_link", False)
            # BUG 2 FIX: Tidak ada di DB meski dalam throttle → bypass, fetch fresh

        result = await self.check_and_save(user_id, force=True)
        # BUG 3 FIX: Simpan timestamp HANYA jika fetch berhasil
        if result is not None:
            self._last_vc_checked[user_id] = now
            self._last_checked[user_id]    = now
        print(
            f"[Monitor {self.chat_id}] VC-join uid={user_id} "
            f"→ bio fresh, has_link={result}"
        )
        return result

    async def check_and_save_typing(self, user_id: int) -> bool | None:
        """
        Handler typing: TIDAK langsung fetch ke Telegram API.

        Perubahan vs versi lama:
          Versi lama: typing → force fetch GetFullUser setiap TYPING_RECHECK_SECS
          Versi baru: typing → catat ke _recent_active → _cache_fill_worker
                      yang isi cache secara pelan di background.

        Ini menghilangkan burst GetFullUser saat banyak user typing bersamaan
        di grup ramai (penyebab utama FloodWait yang dilaporkan).

        Return: data dari cache DB (baca langsung, tidak fetch baru).
        """
        # Catat sebagai aktif → _cache_fill_worker yang jadwalkan fetch
        self._recent_active[user_id] = time.time()

        # Baca dari cache DB — tidak hit API
        try:
            doc = await bio_col.find_one({"chat_id": self.chat_id, "user_id": user_id})
        except Exception:
            doc = None
        return doc.get("has_link", False) if doc else None

    async def check_is_member(self, target: "int | str") -> "bool | None":
        """
        Cek apakah user adalah member grup ini via bot pembantu.

        target bisa:
          - int  → user_id (dari TEXT_MENTION / tg://user?id=)
          - str  → username tanpa @ (dari @mention biasa)

        Alur:
          1. Cek mention_member_cache dulu (DB lokal, tidak hit Telegram)
          2. Cache hit  → return langsung + refresh TTL
          3. Cache miss → panggil get_chat_member via client bot pembantu
          4. Simpan hasilnya ke cache
          5. Return True (member) / False (bukan member) / None (error)

        FloodWait ditangkap dan return None → caller fallback ke bot utama.
        """
        from pyrogram.errors import UserNotParticipant, PeerIdInvalid, RPCError

        # ── 1. Cek cache by user_id ─────────────────────────────────────────
        if isinstance(target, int):
            cached = await mention_cache_get_by_uid(self.chat_id, target)
            if cached is not None:
                # Hit — perbarui TTL supaya entry tidak expire jika masih aktif
                asyncio.create_task(mention_cache_refresh_ttl(self.chat_id, target))
                return cached

        # ── 2. Cek cache by username ────────────────────────────────────────
        elif isinstance(target, str):
            cached = await mention_cache_get_by_username(self.chat_id, target)
            if cached is not None:
                # Refresh TTL lewat username — entry ini (member ATAU bukan
                # member) tetap awet selama masih aktif disebut, walau belum
                # ke-resolve ke user_id.
                asyncio.create_task(mention_cache_refresh_ttl(self.chat_id, username=target))
                return cached

        # ── 3. Cache miss → tanya Telegram via bot pembantu ─────────────────
        try:
            member = await self.client.get_chat_member(self.chat_id, target)
            # Anggota aktif, restricted, dsb = masih member
            is_member = member is not None
            # Ambil info user_id dan username dari hasil
            user_id  = member.user.id       if (member and member.user) else (target if isinstance(target, int) else None)
            username = member.user.username if (member and member.user) else (target if isinstance(target, str) else None)
            # Simpan ke cache
            if user_id:
                await mention_cache_set(
                    self.chat_id, user_id, is_member,
                    username=username,
                )
            return is_member

        except (UserNotParticipant, PeerIdInvalid):
            # Bukan member atau user tidak ada
            user_id  = target if isinstance(target, int) else None
            username = target if isinstance(target, str) else None
            if user_id:
                await mention_cache_set(self.chat_id, user_id, False, username=username)
            return False

        except FloodWait as fw:
            print(
                f"[Monitor {self.chat_id}] check_is_member FloodWait "
                f"{fw.value}s target={target} — skip, fallback ke bot utama"
            )
            return None   # None = sinyal fallback ke bot utama

        except Exception as e:
            print(f"[Monitor {self.chat_id}] check_is_member error target={target}: {e}")
            return None   # None = sinyal fallback ke bot utama

    def _register_handlers(self) -> None:
        """Daftarkan handler Pyrogram ke client instance ini."""
        chat_id = self.chat_id
        monitor = self

        # ── User KIRIM PESAN di grup ──────────────────────────────────────────
        # Catat ke _recent_active (fill loop yang refresh cache).
        # force=False: baca cache dulu, enqueue hanya jika sudah expired.
        @self.client.on_message(filters.chat(chat_id) & (filters.group | filters.forum))
        async def _on_message(client: Client, message: Message):
            user = message.from_user
            if user is None or user.is_bot:
                return
            # Catat sebagai aktif untuk fill loop
            monitor._recent_active[user.id] = time.time()
            # Cek throttle — hanya enqueue jika cache expired
            await monitor.check_and_save(user.id, force=False)

        # ── User JOIN/LEAVE grup → update mention cache + cek bio ──────────
        @self.client.on_chat_member_updated()
        async def _on_join(client: Client, upd: ChatMemberUpdated):
            if upd.chat.id != chat_id:
                return
            if upd.new_chat_member is None:
                return
            user = upd.new_chat_member.user
            if user is None or user.is_bot:
                return

            from pyrogram.enums import ChatMemberStatus
            new_status = upd.new_chat_member.status

            # Member KELUAR / KICK / BAN → tandai is_member=False di cache
            if new_status in (
                ChatMemberStatus.LEFT,
                ChatMemberStatus.BANNED,
                ChatMemberStatus.RESTRICTED,
            ):
                await mention_cache_remove_member(chat_id, user.id)
                return

            # Member JOIN / REJOIN → tandai is_member=True di cache + cek bio
            await mention_cache_set(
                chat_id, user.id, True,
                username=user.username,
            )
            print(
                f"[Monitor {chat_id}] User {user.id} join "
                "→ update mention cache + cek bio (force)"
            )
            await monitor.check_and_save(user.id, force=True)

        # ── Perubahan profil user & TYPING → raw_update handler ──────────────
        @self.client.on_raw_update()
        async def _on_profile_or_typing(client, update, users, chats):
            try:
                # ── Skenario TYPING ──────────────────────────────────────────
                if isinstance(update, raw_types.UpdateUserTyping):
                    user_id = getattr(update, "user_id", None)
                    if user_id and isinstance(user_id, int) and user_id > 0:
                        # Hanya catat + baca cache — TIDAK fetch API
                        await monitor.check_and_save_typing(user_id)
                    return

                # ── Skenario PERUBAHAN PROFIL ─────────────────────────────────
                user_id = None
                if isinstance(update, raw_types.UpdateUserName):
                    user_id = getattr(update, "user_id", None)
                else:
                    type_name = type(update).__name__
                    if "Photo" in type_name or "Profile" in type_name:
                        user_id = getattr(update, "user_id", None)

                if user_id and isinstance(user_id, int) and user_id > 0:
                    known = await bio_col.find_one(
                        {"chat_id": chat_id, "user_id": user_id}
                    )
                    if known:
                        print(
                            f"[Monitor {chat_id}] Profil uid={user_id} "
                            "berubah → re-check"
                        )
                        await monitor.check_and_save(user_id, force=True)
            except Exception as e:
                print(f"[Monitor {chat_id}] raw_update error: {e}")

        self._raw_handler_registered = True


# ══════════════════════════════════════════════════════════════════════════════
# MANAJER INSTANCE — LOAD / RELOAD / STOP
# ══════════════════════════════════════════════════════════════════════════════

async def _load_instances_from_db() -> None:
    """
    Baca semua grup Security OS aktif dari DB.
    Untuk tiap grup yang punya monitor_token → spawn MonitorInstance.
    Dipanggil saat startup.
    """
    # Pastikan TTL index sudah ada sebelum instance mulai menulis
    await _ensure_ttl_index()

    async for doc in sec_col.find({"monitor_token": {"$exists": True, "$ne": ""}}):
        chat_id = doc.get("chat_id") or doc.get("_id")
        token   = doc.get("monitor_token", "")
        bot_id  = doc.get("monitor_bot_id", 0)
        if not chat_id or not token:
            continue
        async with _instances_lock:
            if chat_id not in _active_instances:
                await _spawn_instance(chat_id, token, bot_id)


async def _spawn_instance(chat_id: int, token: str, bot_id: int) -> bool:
    """
    Buat dan start MonitorInstance baru.
    Return True jika berhasil.
    """
    instance = MonitorInstance(chat_id, token, bot_id)
    ok = await instance.start()
    if ok:
        _active_instances[chat_id] = instance
    return ok


async def _stop_instance(chat_id: int) -> None:
    """Stop dan hapus MonitorInstance untuk grup ini."""
    instance = _active_instances.pop(chat_id, None)
    if instance:
        await instance.stop()


async def reload_monitor_instances() -> None:
    """
    Reload semua instance dari DB.
    Stop instance yang token-nya sudah dihapus,
    spawn instance baru untuk grup yang belum punya instance.
    """
    await _ensure_ttl_index()

    db_chat_ids: set[int] = set()
    async for doc in sec_col.find({"monitor_token": {"$exists": True, "$ne": ""}}):
        chat_id = doc.get("chat_id") or doc.get("_id")
        token   = doc.get("monitor_token", "")
        bot_id  = doc.get("monitor_bot_id", 0)
        if not chat_id or not token:
            continue
        db_chat_ids.add(chat_id)
        async with _instances_lock:
            if chat_id not in _active_instances:
                await _spawn_instance(chat_id, token, bot_id)

    # Stop instance yang sudah tidak ada di DB
    stale = set(_active_instances.keys()) - db_chat_ids
    for chat_id in stale:
        async with _instances_lock:
            await _stop_instance(chat_id)


async def spawn_monitor_for_group(chat_id: int, token: str, bot_id: int) -> bool:
    """
    Spawn MonitorInstance untuk grup baru (dipanggil saat admin setup token).
    Stop instance lama jika ada (token mungkin diganti).
    """
    await _ensure_ttl_index()
    async with _instances_lock:
        if chat_id in _active_instances:
            await _stop_instance(chat_id)
        return await _spawn_instance(chat_id, token, bot_id)


async def stop_monitor_for_group(chat_id: int) -> None:
    """Stop MonitorInstance untuk grup ini (dipanggil saat Security OS dinonaktifkan)."""
    async with _instances_lock:
        await _stop_instance(chat_id)


def get_active_instance_count() -> int:
    return len(_active_instances)


def get_active_chat_ids() -> list[int]:
    return list(_active_instances.keys())


async def save_all_sessions() -> None:
    """
    Backup session SEMUA monitor instance yang sedang aktif ke MongoDB.
    Dipanggil:
      - Secara periodik (lihat _periodic_session_backup di bawah)
      - Saat graceful shutdown proses utama (dipanggil dari antigcast.py)
        agar peer cache yang ditemui sejak start tidak hilang saat redeploy.
    """
    for instance in list(_active_instances.values()):
        try:
            await instance._save_session()
        except Exception as e:
            print(f"[Monitor {instance.chat_id}] ⚠️  Gagal backup session: {e}")


async def _periodic_session_backup(interval_secs: int = 20 * 60) -> None:
    """
    Backup session semua monitor aktif setiap interval_secs (default 20 menit).
    Sama seperti mekanisme di bot utama — peer baru yang ditemui monitor selama
    berjalan (member baru yang masuk grup, dll) ikut terbawa ke redeploy berikutnya.
    """
    while True:
        await asyncio.sleep(interval_secs)
        await save_all_sessions()
        print(f"[Monitor] 🔄 Periodic backup session selesai ({len(_active_instances)} instance).")


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API — dipanggil dari bio.py / video_call.py
# ══════════════════════════════════════════════════════════════════════════════

async def force_check_user(chat_id: int, user_id: int) -> bool | None:
    """
    Paksa re-check bio user via MonitorInstance aktif untuk grup ini.

    Dipanggil oleh bio.py HANYA saat cache benar-benar kosong (None dari DB).
    Lewat antrian _bio_worker — tidak bypass rate-limit.

    Alur:
      1. Bot utama deteksi pesan dari user X di grup A
      2. Query bio_profiles {chat_id: A, user_id: X} → None (belum ada)
      3. Bot utama panggil force_check_user(A, X)
      4. MonitorInstance enqueue user X → _bio_worker fetch bio
      5. Simpan ke bio_profiles dengan expires_at = sekarang + TTL + jitter
      6. Return has_link → bot utama hapus pesan jika True

    Return:
      True  → ada link di bio
      False → tidak ada link
      None  → instance tidak aktif atau gagal fetch
    """
    instance = _active_instances.get(chat_id)
    if instance is None:
        return None
    try:
        return await instance.check_and_save(user_id, force=True)
    except Exception as e:
        print(f"[MonitorQuery] force_check_user chat={chat_id} uid={user_id}: {e}")
        return None


async def force_check_vc_join(chat_id: int, user_id: int) -> bool | None:
    """
    Paksa re-check bio user saat NAIK KE VOICE CHAT.
    Cache khusus VC (VC_JOIN_RECHECK_SECS = BIO_TTL_SECS default).

    Dipanggil dari video_call.py → saat user join VC.
    """
    instance = _active_instances.get(chat_id)
    if instance is None:
        return None
    try:
        return await instance.check_and_save_vc(user_id)
    except Exception as e:
        print(f"[MonitorQuery] force_check_vc_join chat={chat_id} uid={user_id}: {e}")
        return None


async def query_bio(chat_id: int, user_id: int) -> bool | None:
    """
    Baca hasil cek bio dari DB untuk pasangan (chat_id, user_id).
    Data ini ditulis oleh MonitorInstance grup yang bersangkutan.

    Karena data ber-TTL, dokumen yang sudah expired otomatis tidak ada
    di DB → return None → bot utama akan trigger force_check_user.

    Return:
      True  → ada link di bio
      False → tidak ada link di bio
      None  → data belum ada atau sudah expired → perlu force_check_user
    """
    try:
        doc = await bio_col.find_one(
            {"chat_id": chat_id, "user_id": user_id}
        )
    except Exception as e:
        print(
            f"[MonitorQuery] Gagal query bio "
            f"chat={chat_id} uid={user_id}: {e}"
        )
        return None

    if not doc:
        return None

    return doc.get("has_link", False)


async def check_member_via_monitor(chat_id: int, target: "int | str") -> "bool | None":
    """
    Cek apakah user adalah member grup via bot pembantu (MonitorInstance).

    Dipanggil oleh antispam_queue._is_external_mention() sebagai pengganti
    langsung client.get_chat_member() dari bot utama.

    target:
      - int  → user_id (dari TEXT_MENTION / tg://user?id=)
      - str  → username tanpa @ (dari @mention biasa)

    Return:
      True  → user adalah member grup ini
      False → user bukan member (external mention)
      None  → MonitorInstance tidak aktif atau error (fallback ke bot utama)
    """
    instance = _active_instances.get(chat_id)
    if instance is None:
        return None   # tidak ada bot pembantu → fallback ke bot utama
    try:
        return await instance.check_is_member(target)
    except Exception as e:
        print(f"[MonitorQuery] check_member_via_monitor chat={chat_id} target={target}: {e}")
        return None


async def query_admin_bio_ok(chat_id: int, user_id: int) -> "bool | None":
    """
    Baca hasil cek "Bio Admin Wajib" (NewsCore) dari DB untuk pasangan
    (chat_id, user_id). Ditulis bersamaan dengan has_link oleh
    MonitorInstance.check_and_save() — lihat field admin_bio_ok.

    Return:
      True  → user adalah admin NewsCore aktif & bio memenuhi teks wajib
      False → user adalah admin NewsCore aktif & bio TIDAK memenuhi teks wajib
              → bot utama harus panggil core.ns_bio_guard.enforce_admin_bio()
      None  → bukan admin NewsCore / data belum ada — tidak perlu tindakan
    """
    try:
        doc = await bio_col.find_one(
            {"chat_id": chat_id, "user_id": user_id}
        )
    except Exception as e:
        print(
            f"[MonitorQuery] Gagal query admin_bio_ok "
            f"chat={chat_id} uid={user_id}: {e}"
        )
        return None

    if not doc:
        return None

    return doc.get("admin_bio_ok")


# ══════════════════════════════════════════════════════════════════════════════
# STARTUP — ENTRY POINT (jalankan sebagai proses terpisah)
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    """
    Entry point jika monitor_bot_reference.py dijalankan langsung (standalone).
    Dalam deployment normal, file ini di-import oleh antigcast.py.
    """
    from database import setup_db
    await setup_db()
    await _load_instances_from_db()
    print(f"[Monitor] {get_active_instance_count()} instance aktif.")
    await idle()


if __name__ == "__main__":
    asyncio.run(main())
