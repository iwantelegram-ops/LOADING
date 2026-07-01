"""
core/workshop_pool.py — "Bengkel": Pool Token Backup untuk GetFullUser
────────────────────────────────────────────────────────────────────────
LATAR BELAKANG:
  GetFullUser adalah API call paling "mahal" (paling sering kena FloodWait)
  yang dilakukan bot. Skema lama (monitor_bot_reference.py) sudah memisahkan
  ini dari bot utama — TAPI 1 token monitor = 1 grup tetap. Saat grup makin
  banyak dan ramai, masing-masing token monitor tetap bisa kena FloodWait
  sendiri-sendiri karena menanggung SEMUA user di grupnya sendirian.

STRATEGI "BENGKEL":
  Sediakan N token cadangan (TOKEN_BACKUP1, TOKEN_BACKUP2, ... tanpa batas)
  yang TIDAK terikat ke grup manapun. Saat ada permintaan GetFullUser:
    1. Pool pilih token yang sedang PALING IDLE (bukan FloodWait, paling
       lama tidak dipakai) — bukan round-robin buta tanpa syarat.
    2. Kalau token itu kena FloodWait saat dipakai, ditandai "busy_until"
       dan pool otomatis coba token lain berikutnya (bukan tunggu/gagal).
    3. Hasil (bio + has_link) ditulis ke bio_profiles lewat modul database
       yang sudah ada — otomatis ikut sharding multi-Mongo, TTL, dan semua
       consumer (bio.py, vip_bio_guard.py, dst) tidak perlu diubah sama sekali.

  Bot utama TIDAK PERNAH memanggil GetFullUser sendiri — ia hanya:
    a. Baca bio_profiles (cache hit → langsung pakai)
    b. Cache miss → minta workshop_pool.fetch(chat_id, user_id)
    c. Eksekusi hapus pesan / restrict berdasarkan hasil

KAPAN DIPAKAI vs MonitorInstance (skema lama):
  - Grup yang SUDAH punya MonitorInstance aktif (token sendiri) → tetap
    pakai jalur lama (force_check_user di monitor_bot_reference.py),
    karena token itu sudah "didedikasikan" untuk grup itu.
  - Grup yang BELUM/TIDAK punya MonitorInstance, atau MonitorInstance-nya
    sedang FloodWait → fallback ke workshop_pool ini.
  Titik integrasi: bio.py memanggil keduanya secara berurutan (lihat
  PERUBAHAN DI bio.py di bagian bawah file ini sebagai referensi, modul
  ini sendiri berdiri independen).

.env BARU:
  TOKEN_BACKUP1=123456:ABC-...
  TOKEN_BACKUP2=789012:DEF-...
  ...(tanpa batas, auto-terdeteksi berurutan, berhenti di nomor kosong)

TIDAK PERLU GRUP TEMPAT BOT INI JADI ADMIN:
  Token backup HANYA dipakai untuk GetFullUser (bisa dipanggil terhadap
  user manapun yang bisa di-resolve, tidak perlu bot jadi member grup),
  bukan untuk baca histori pesan grup. Ini sama dengan strategi 4-langkah
  resolve_peer yang sudah dipakai di monitor_bot_reference.py.
"""

from __future__ import annotations

import os
import time
import asyncio
from pathlib import Path as _Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=_Path(__file__).resolve().parent.parent / ".env", override=False)

API_ID   = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")

# Jeda minimum antar pemakaian SATU token (selain FloodWait), agar tetap
# sopan ke Telegram walau tidak sedang FloodWait — mencegah pool memukul
# token yang sama bertubi-tubi hanya karena dia "paling idle" di awal.
_TOKEN_MIN_GAP_SECS = float(os.environ.get("WORKSHOP_TOKEN_MIN_GAP_SECS", 1.0))

# Timeout keseluruhan satu permintaan fetch (semua percobaan token gabungan)
_FETCH_TIMEOUT_SECS = float(os.environ.get("WORKSHOP_FETCH_TIMEOUT_SECS", 12.0))


def _collect_backup_tokens() -> list[str]:
    """
    Kumpulkan TOKEN_BACKUP1, TOKEN_BACKUP2, ... dari .env secara berurutan
    tanpa batas. Berhenti saat menemukan nomor yang tidak diisi (sama
    seperti pola MONGO_URL_n di core/mongo_shard.py — konsisten).
    """
    tokens: list[str] = []
    n = 1
    while True:
        val = os.environ.get(f"TOKEN_BACKUP{n}", "").strip()
        if not val:
            break
        tokens.append(val)
        n += 1
    return tokens


BACKUP_TOKENS: list[str] = _collect_backup_tokens()


class _WorkshopWorker:
    """Satu token backup = satu Client siap pakai, idle sampai dipanggil."""

    __slots__ = ("index", "token", "client", "busy_until", "last_used", "lock", "_started")

    def __init__(self, index: int, token: str):
        self.index      = index
        self.token      = token
        self.busy_until = 0.0   # monotonic timestamp; 0 = tidak FloodWait
        self.last_used  = 0.0
        self.lock        = asyncio.Lock()  # 1 worker hanya proses 1 fetch sekaligus
        self._started    = False
        self.client      = None  # lazy: dibuat saat start()

    async def start(self):
        if self._started:
            return
        from pyrogram import Client  # import lokal: hindari beban import saat modul ini di-import tapi tidak dipakai
        session_name = f"workshop_backup_{self.index}"
        self.client = Client(
            session_name,
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=self.token,
            in_memory=False,
        )
        await self.client.start()
        self._started = True
        print(f"[Workshop] ✅ Worker #{self.index} siap (token backup).")

    async def stop(self):
        if self._started and self.client:
            try:
                await self.client.stop()
            except Exception:
                pass
            self._started = False

    def is_available(self, now: float) -> bool:
        return now >= self.busy_until and not self.lock.locked()

    def mark_floodwait(self, seconds: float):
        self.busy_until = time.monotonic() + seconds


class WorkshopPool:
    """
    Manajer N worker token backup. Pilih worker idle, fetch GetFullUser,
    otomatis ganti worker lain kalau kena FloodWait, simpan hasil ke
    bio_profiles via database.py (sharded otomatis).
    """

    def __init__(self, tokens: list[str]):
        self._workers: list[_WorkshopWorker] = [
            _WorkshopWorker(i, tok) for i, tok in enumerate(tokens)
        ]
        self._started = False
        self._start_lock = asyncio.Lock()

    @property
    def size(self) -> int:
        return len(self._workers)

    async def start_all(self):
        """Login semua worker sekali di awal (dipanggil dari main() startup)."""
        async with self._start_lock:
            if self._started or not self._workers:
                self._started = True
                return
            results = await asyncio.gather(
                *[w.start() for w in self._workers], return_exceptions=True
            )
            ok = sum(1 for r in results if not isinstance(r, Exception))
            print(f"[Workshop] Pool siap: {ok}/{len(self._workers)} worker token backup aktif.")
            self._started = True

    async def stop_all(self):
        await asyncio.gather(*[w.stop() for w in self._workers], return_exceptions=True)

    def _pick_worker(self, exclude: set[int]) -> "_WorkshopWorker | None":
        """Pilih worker idle dengan last_used paling lama (paling 'segar'),
        skip yang sedang FloodWait/locked atau sudah dicoba (exclude)."""
        now = time.monotonic()
        candidates = [
            w for w in self._workers
            if w.index not in exclude and w.is_available(now)
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda w: w.last_used)
        return candidates[0]

    async def fetch_full_user(self, chat_id: int, user_id: int) -> str | None:
        """
        Ambil bio user via salah satu token backup yang idle.
        Strategi sama dengan monitor_bot_reference._fetch_bio (4 langkah),
        disederhanakan jadi 2 langkah utama yang paling sering berhasil
        (resolve_peer langsung, lalu InputUser access_hash=0) karena pool
        ini tidak terikat ke satu grup tertentu (tidak punya get_chat_member
        yang relevan untuk member-warming).

        Return: string bio (boleh "") atau None jika semua token gagal/timeout.
        """
        if not self._workers:
            return None
        if not self._started:
            await self.start_all()

        from pyrogram.errors import FloodWait, PeerIdInvalid
        from pyrogram.raw import functions as raw_fns
        from pyrogram.raw.types import InputUser as _RawInputUser

        tried: set[int] = set()
        deadline = time.monotonic() + _FETCH_TIMEOUT_SECS

        while time.monotonic() < deadline:
            worker = self._pick_worker(tried)
            if worker is None:
                # semua worker sedang FloodWait/locked → tunggu sebentar, coba lagi
                await asyncio.sleep(0.3)
                if len(tried) >= len(self._workers):
                    tried.clear()  # mungkin FloodWait sudah lewat, ulangi dari awal
                continue

            tried.add(worker.index)
            async with worker.lock:
                # jaga jarak minimum antar pemakaian token yang sama
                gap = _TOKEN_MIN_GAP_SECS - (time.monotonic() - worker.last_used)
                if gap > 0:
                    await asyncio.sleep(gap)
                worker.last_used = time.monotonic()

                try:
                    peer = await worker.client.resolve_peer(user_id)
                except (PeerIdInvalid, KeyError):
                    peer = _RawInputUser(user_id=user_id, access_hash=0)
                except FloodWait as fw:
                    worker.mark_floodwait(fw.value + 1)
                    continue
                except Exception:
                    continue

                try:
                    full = await worker.client.invoke(raw_fns.users.GetFullUser(id=peer))
                    bio = getattr(full.full_user, "about", None) or ""
                    return bio
                except FloodWait as fw:
                    worker.mark_floodwait(fw.value + 1)
                    print(f"[Workshop] Worker #{worker.index} FloodWait {fw.value}s → ganti worker lain.")
                    continue
                except Exception:
                    # access_hash=0 gagal juga → coba get_users sebagai last resort
                    try:
                        users_result = await worker.client.get_users(user_id)
                        u = users_result[0] if isinstance(users_result, list) else users_result
                        if u:
                            peer2 = await worker.client.resolve_peer(u.id)
                            full = await worker.client.invoke(raw_fns.users.GetFullUser(id=peer2))
                            return getattr(full.full_user, "about", None) or ""
                    except FloodWait as fw2:
                        worker.mark_floodwait(fw2.value + 1)
                    except Exception:
                        pass
                    continue

        print(f"[Workshop] ⚠️  Semua token backup gagal/timeout untuk uid={user_id}.")
        return None

    async def check_and_save(self, chat_id: int, user_id: int, ttl_secs: int = 300) -> bool | None:
        """
        Fetch bio via pool lalu simpan ke bio_profiles — format dokumen SAMA
        dengan yang ditulis MonitorInstance, supaya semua consumer (bio.py,
        vip_bio_guard.py, unmutemic.py, dst) membaca format yang identik
        tanpa tahu apakah data datang dari MonitorInstance atau dari Bengkel.

        Return: True (ada link), False (tidak ada link), None (gagal total).
        """
        bio = await self.fetch_full_user(chat_id, user_id)
        if bio is None:
            return None

        # Import lokal untuk hindari circular import (database.py tidak
        # perlu tahu soal workshop_pool, hanya workshop_pool yang tahu database).
        from database import db
        import re as _re
        from datetime import datetime, timedelta, timezone

        link_pattern = _re.compile(
            r"(@\S+|https?://\S+|t\.me/\S+|bit\.ly/\S+|linktr\.ee/\S+)",
            _re.IGNORECASE,
        )
        has_link = bool(link_pattern.search(bio)) if bio else False
        now = time.time()
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_secs)

        try:
            await db["bio_profiles"].update_one(
                {"chat_id": chat_id, "user_id": user_id},
                {"$set": {
                    "chat_id": chat_id,
                    "user_id": user_id,
                    "has_link": has_link,
                    "bio": bio,
                    "checked_at": now,
                    "updated_at": now,
                    "expires_at": expires_at,
                    "source": "workshop",  # jejak audit: data ini dari Bengkel, bukan MonitorInstance
                }},
                upsert=True,
            )
        except Exception as e:
            print(f"[Workshop] Gagal simpan bio_profiles chat={chat_id} uid={user_id}: {e}")

        return has_link


# ── Singleton pool — dipakai langsung oleh bio.py / antispam.py ──────────────
workshop_pool = WorkshopPool(BACKUP_TOKENS)
