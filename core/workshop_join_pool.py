"""
core/workshop_join_pool.py — "Bengkel Join": Bot Bengkel Masuk Grup Sementara
════════════════════════════════════════════════════════════════════════════════
LATAR BELAKANG:
  core/workshop_pool.py (mode lama, tetap ada & tetap dipakai sebagai fallback
  di modul ini) adalah pool token backup yang berdiri BEBAS — tidak pernah
  jadi member grup, cuma fetch GetFullUser pakai resolve_peer/access_hash=0.
  Itu cukup untuk kasus umum, tapi untuk grup yang user-nya banyak dan PRIVASI
  BIO-nya ketat (about hanya kebaca oleh member grup yang sama), pool lama
  bisa tetap gagal resolve.

DESAIN (modul ini):
  1. Tiap grup TETAP punya bot pemantau (MonitorInstance) sendiri-sendiri,
     seperti sebelumnya — token per grup. instance punya
     is_floodwait()/mark_floodwait()/frozen (lihat
     security_os/monitor_bot_reference.py).

  2. Begitu MonitorInstance grupA terdeteksi FloodWait:
       a. drain_queue() — buang semua antrian lama instance itu (future-nya
          di-resolve None). User yang masih perlu dicek akan otomatis masuk
          lagi ke antrian lewat _recent_active / request baru selama Bengkel
          aktif — bukan tugas drain ini untuk migrasikan manual.
       b. instance.frozen = True — instance asli BERHENTI konsumsi
          _bio_queue & _cache_fill_worker (urusan BIO saja). Fungsi LAIN
          instance (event handler join/leave, check_is_member, dsb — apapun
          yang tidak lewat _bio_worker/GetFullUser) TETAP berjalan normal,
          tidak terpengaruh frozen sama sekali — lihat monitor_bot_reference.py.
       c. Pool pilih 1 bot Bengkel yang BENAR-BENAR IDLE (tidak sedang di
          grup manapun) secara round-robin, skip yang juga FloodWait.
       d. Userbot (akun biasa — bot tidak bisa invite bot lain) coba
          add_chat_members() Bengkel itu ke grupA.
       e. WAJIB VERIFIKASI: setelah add_chat_members(), pool mengecek ULANG
          via get_chat_member() bahwa Bengkel benar-benar berstatus member
          di grupA SEBELUM Bengkel diizinkan kerja apapun di grup itu. Kalau
          verifikasi gagal (add_chat_members gagal/ditolak, atau status
          member tidak terkonfirmasi) → Bengkel TIDAK dianggap assigned ke
          grupA, kembali idle, grupA balik ke antrian FIFO (atau fallback
          workshop_pool standalone). Bengkel TIDAK PERNAH "kerja seolah-olah
          masuk" tanpa konfirmasi join — ini constraint keras, bukan upaya
          terbaik.
       f. Begitu confirmed joined → grupA masuk mode KERJA GABUNGAN (lihat
          poin 5 di bawah): instance asli (pemantau) dan Bengkel yang sudah
          join bergantian satu fetch per giliran (round-robin), BUKAN
          "1 floodwait ganti 1" — keduanya tetap aktif berbarengan,
          melebarkan jeda efektif antar fetch sehingga lebih jarang kena
          FloodWait lagi.

  3. Kalau SEMUA Bengkel sedang assigned ke grup lain → grup yang butuh
     bantuan masuk antrian FIFO (_pending_groups), DAN sementara menunggu,
     fallback ke workshop_pool (mode lama, standalone, tidak join grup) lewat
     rantai fallback bio.py sendiri — supaya user tidak nunggu tanpa ada yang
     mengecek bio sama sekali. Antrian grup ini DIBATALKAN (dibuang dari
     _pending_groups) begitu instance pemantau aslinya tidak lagi FloodWait
     sebelum kebagian slot Bengkel — tidak ada gunanya menunggu Bengkel kalau
     pemantau sendiri sudah pulih duluan.

  4. GRUP RAMAI (>1 Bengkel untuk 1 grup): selama grupA SUDAH dalam mode
     kerja gabungan (≥1 Bengkel confirmed joined) dan instance pemantau
     FloodWait LAGI (sinyal 1 Bengkel belum cukup meredam beban grup itu) →
     pool mencoba menambah 1 Bengkel idle lagi ke grupA dengan prosedur
     join+verifikasi yang SAMA seperti poin 2 (d-f). Tidak ada batas atas
     selain jumlah Bengkel yang tersedia di pool. Semua peserta (pemantau +
     N Bengkel) ikut rotasi round-robin yang sama.

  5. KERJA GABUNGAN (round-robin, bukan "1 ganti 1"):
     Begitu ≥1 Bengkel confirmed joined ke grupA, satu KOORDINATOR per-grup
     (_GroupWorkRotation) mengambil alih konsumsi _bio_queue & pengisian
     cache dari _recent_active milik instance asli. Tiap giliran, koordinator
     memilih peserta SELANJUTNYA dalam urutan round-robin
     (pemantau → Bengkel#1 → Bengkel#2 → ... → pemantau → ...), tapi:
       - Kalau peserta yang gilirannya tiba SEDANG FloodWait (is_floodwait()
         miliknya sendiri), giliran itu DI-SKIP — TIDAK menghentikan rotasi,
         lanjut langsung ke peserta berikutnya dalam urutan yang sama.
       - 1 giliran = 1 fetch (1 user), baru lanjut ke peserta berikutnya.
     Efeknya: kalau ada K peserta aktif, jeda EFEKTIF antar request yang
     dilihat Telegram per-client adalah ~K × _BIO_QUEUE_DELAY — jauh lebih
     longgar daripada 1 client kerja sendirian, sehingga jauh lebih jarang
     memicu FloodWait baru.
     Instance asli SENDIRI tidak ikut konsumsi _bio_queue selagi frozen
     (_bio_worker miliknya idle-poll seperti sebelumnya) — yang mewakilkan
     "giliran pemantau" dalam rotasi adalah koordinator ini, memakai Client
     pemantau asli (instance.client), bukan _bio_worker instance.

  6. Begitu grupA dianggap "aman" (instance asli tidak FloodWait lagi selama
     _SAFE_DURATION_SECS berturut-turut, DIHITUNG SETELAH SEMUA Bengkel di
     grup itu juga tidak FloodWait) → SEMUA Bengkel grup itu leave_chat(),
     instance asli di-unfreeze (frozen=False), Bengkel-Bengkel itu kembali
     idle dan boleh diambil grup lain di antrian FIFO. Leave dilakukan
     satu-persatu dengan jeda (_LEAVE_STAGGER_SECS).

RESOLVABILITY (PRASYARAT add_chat_members):
  Bot tidak bisa di-add ke grup oleh bot lain kalau belum pernah saling
  "ketemu" sama sekali (Telegram perlu access_hash). Solusinya: begitu
  Bengkel start(), dia kirim 1x pesan ke bot utama (lewat DM, pakai
  OWNER_BOT_ID yang diisi otomatis dari app.start()) — sekali saja,
  cukup untuk membuat Bengkel "resolvable" oleh bot utama selamanya
  (access_hash di-cache Telegram di kedua sisi).

  PRASYARAT LAIN: userbot wajib berstatus admin DENGAN hak invite_users di
  grup yang bersangkutan supaya add_chat_members() berhasil. Modul ini
  mengecek is_userbot_admin(chat_id) (video_call.py) SEBELUM mencoba
  add_chat_members() — kalau userbot bukan admin di grup itu (termasuk
  ketika hak admin/invite userbot DICABUT PAKSA di tengah jalan), percobaan
  invite di-SKIP SAMA SEKALI, tidak ada usaha invite yang dipaksakan. Kalau
  status admin masih ada tapi privilege invite spesifik saja yang dicabut,
  add_chat_members() akan gagal dengan exception dari Telegram — tertangkap
  oleh try/except di _attempt_join_and_verify, tidak fatal, grup tetap lanjut
  lewat fallback (poin 3).

KONSUMSI API:
  - drain_queue(): 0 API call (operasi lokal, cuma asyncio.Queue).
  - add_chat_members / get_chat_member (verifikasi) / leave_chat: masing-
    masing 1 API call, hanya terjadi saat transisi (bukan per-pesan).
  - Rotasi GetFullUser: sama seperti _bio_worker biasa, rate limited
    _BIO_QUEUE_DELAY per fetch per peserta giliran.

.env (semua punya default aman):
  WORKSHOP_SAFE_SECS          — berapa lama grup harus "aman" sebelum
                                 Bengkel ditarik keluar (default: 120)
  WORKSHOP_LEAVE_STAGGER_SECS — jeda antar leave_chat beruntun (default: 5)
  WORKSHOP_POLL_INTERVAL_SECS — interval cek FloodWait semua instance
                                 (default: 5)
  WORKSHOP_JOIN_VERIFY_RETRIES — berapa kali retry get_chat_member sebelum
                                 join dianggap gagal (default: 3)
  WORKSHOP_JOIN_VERIFY_DELAY_SECS — jeda antar retry verifikasi (default: 1.5)
"""

from __future__ import annotations

import os
import time
import asyncio
from pathlib import Path as _Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=_Path(__file__).resolve().parent.parent / ".env", override=False)

_SAFE_DURATION_SECS         = float(os.environ.get("WORKSHOP_SAFE_SECS", 120))
_LEAVE_STAGGER_SECS         = float(os.environ.get("WORKSHOP_LEAVE_STAGGER_SECS", 5))
_POLL_INTERVAL_SECS         = float(os.environ.get("WORKSHOP_POLL_INTERVAL_SECS", 5))
_BIO_QUEUE_DELAY            = float(os.environ.get("BIO_QUEUE_DELAY", 1.5))
_JOIN_VERIFY_RETRIES        = int(os.environ.get("WORKSHOP_JOIN_VERIFY_RETRIES", 3))
_JOIN_VERIFY_DELAY_SECS     = float(os.environ.get("WORKSHOP_JOIN_VERIFY_DELAY_SECS", 1.5))


class _BengkelBot:
    """
    Satu token backup = satu Client Bengkel yang BISA join/leave grup.
    Idle sampai ditugaskan ke 1 grup; hanya boleh ditugaskan ke 1 grup
    dalam satu waktu (dijamin oleh WorkshopJoinPool, bukan oleh kelas ini).

    `joined` HANYA True setelah verifikasi get_chat_member() berhasil
    mengonfirmasi status member di `assigned_chat_id` — ini gate yang
    dipakai _GroupWorkRotation untuk menentukan siapa boleh ikut rotasi
    kerja. Bengkel yang assigned tapi belum/tidak confirmed joined TIDAK
    pernah dianggap peserta rotasi.
    """

    __slots__ = (
        "index", "token", "client", "user_id", "username",
        "assigned_chat_id", "joined", "busy_until", "_started",
        "_safe_since",
    )

    def __init__(self, index: int, token: str):
        self.index            = index
        self.token             = token
        self.client            = None
        self.user_id: int | None  = None
        self.username: str | None = None
        self.assigned_chat_id: int | None = None  # None = idle
        self.joined: bool      = False  # True hanya setelah confirmed member
        self.busy_until        = 0.0   # FloodWait pada Bengkel ini sendiri
        self._started          = False
        self._safe_since: float | None = None  # monotonic ts sejak grup dianggap aman

    @property
    def is_idle(self) -> bool:
        return self.assigned_chat_id is None

    def is_floodwait(self, now: float | None = None) -> bool:
        return (now if now is not None else time.monotonic()) < self.busy_until

    def mark_floodwait(self, seconds: float) -> None:
        self.busy_until = time.monotonic() + seconds

    async def start(self, main_app) -> bool:
        if self._started:
            return True
        from pyrogram import Client
        from pyrogram.errors import FloodWait
        session_name = f"workshop_join_{self.index}"
        self.client = Client(
            session_name,
            api_id=int(os.environ.get("API_ID", 0)),
            api_hash=os.environ.get("API_HASH", ""),
            bot_token=self.token,
            in_memory=False,
        )
        try:
            await self.client.start()
            me = await self.client.get_me()
            self.user_id  = me.id
            self.username = me.username
            self._started = True

            # ── Bootstrap resolvability ──────────────────────────────────
            # Bot tidak bisa DM ke bot lain (USER_IS_BOT).
            # get_users(integer_id) gagal kalau belum ada shared context.
            # Solusi: pakai @username bot bengkel → trigger ResolveUsername
            # RPC di sisi bot utama, tidak butuh shared context apapun.
            # Setelah resolve, access_hash ter-cache → add_chat_members bisa.
            if main_app is not None and self.username:
                try:
                    await main_app.get_users(f"@{self.username}")
                except Exception as e:
                    print(f"[BengkelJoin #{self.index}] ⚠️  Bootstrap resolve gagal: {e}")

            print(f"[BengkelJoin #{self.index}] ✅ Siap (user_id={self.user_id}).")
            return True
        except FloodWait as fw:
            # Login (auth.ImportBotAuthorization) sendiri kena rate-limit
            # Telegram — BUKAN error permanen. Simpan waktu tunggu di
            # busy_until (mekanisme yang sama dipakai untuk FloodWait saat
            # Bengkel sudah bertugas) supaya _monitor_loop tahu kapan boleh
            # coba start() lagi otomatis, tanpa perlu restart bot manual.
            self.mark_floodwait(fw.value)
            print(
                f"[BengkelJoin #{self.index}] ❌ Gagal start (FloodWait {fw.value}s) — "
                f"akan dicoba otomatis lagi setelah waktu tunggu ini selesai."
            )
            return False
        except Exception as e:
            print(f"[BengkelJoin #{self.index}] ❌ Gagal start: {e}")
            return False

    async def stop(self):
        if self._started and self.client:
            try:
                await self.client.stop()
            except Exception:
                pass
            self._started = False


class _GroupWorkRotation:
    """
    Koordinator round-robin untuk SATU grup yang sedang dalam mode kerja
    gabungan (instance pemantau frozen + ≥1 Bengkel confirmed joined).

    Peserta = [instance pemantau (diwakili koordinator ini, pakai
    instance.client), Bengkel#1, Bengkel#2, ...] — urutan stabil sesuai
    urutan join. Tiap giliran konsumsi 1 item dari _bio_queue / 1 kandidat
    fill dari _recent_active, lalu maju ke peserta berikutnya. Peserta yang
    gilirannya tiba tapi sedang is_floodwait() di-SKIP (giliran lanjut ke
    peserta berikutnya, BUKAN berhenti / nunggu).

    Ini BUKAN "1 floodwait ganti 1" — instance pemantau & semua Bengkel
    yang joined tetap sama-sama aktif kerja, bergantian, melebarkan jeda
    efektif per-client supaya lebih jarang floodwait lagi.
    """

    def __init__(self, chat_id: int, instance):
        self.chat_id  = chat_id
        self.instance = instance
        self.bengkels: list[_BengkelBot] = []   # Bengkel confirmed joined, urut
        self._rr_index = 0
        self._task: asyncio.Task | None = None
        self._stopped = False

    def add_bengkel(self, bengkel: "_BengkelBot") -> None:
        if bengkel not in self.bengkels:
            self.bengkels.append(bengkel)

    def remove_bengkel(self, bengkel: "_BengkelBot") -> None:
        if bengkel in self.bengkels:
            self.bengkels.remove(bengkel)

    @property
    def participant_count(self) -> int:
        return 1 + len(self.bengkels)  # 1 = instance pemantau

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stopped = False
            self._task = asyncio.create_task(
                self._loop(), name=f"bengkel_rotation_{abs(self.chat_id)}"
            )

    async def stop(self) -> None:
        self._stopped = True
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    def _participants(self) -> list:
        """['pemantau', bengkel1, bengkel2, ...] — urutan stabil."""
        return ["pemantau"] + list(self.bengkels)

    async def _loop(self) -> None:
        instance = self.instance
        print(f"[BengkelJoin] Rotasi kerja gabungan dimulai grup {self.chat_id} "
              f"({self.participant_count} peserta).")
        last_fill_check = 0.0

        while not self._stopped and instance.frozen:
            participants = self._participants()
            if not participants:
                await asyncio.sleep(1.0)
                continue

            # Maju satu giliran (round-robin sederhana berbasis index).
            self._rr_index %= len(participants)
            participant = participants[self._rr_index]
            self._rr_index = (self._rr_index + 1) % len(participants)

            # Peserta sedang floodwait sendiri → skip giliran ini, LANJUT
            # ke peserta berikutnya tanpa menunggu / menghentikan rotasi.
            if participant != "pemantau" and participant.is_floodwait():
                await asyncio.sleep(0)  # yield, lanjut ke giliran berikutnya
                continue
            if participant == "pemantau" and instance.is_floodwait():
                await asyncio.sleep(0)
                continue

            did_work = False
            try:
                # 1. Prioritas: layani _bio_queue (request mendesak — join,
                #    VC, force_check_user) lebih dulu.
                try:
                    user_id, future = instance._bio_queue.get_nowait()
                    await self._fetch_and_save(participant, instance, user_id, future)
                    instance._bio_queue_pending.discard(user_id)
                    instance._bio_queue.task_done()
                    did_work = True
                except asyncio.QueueEmpty:
                    pass

                # 2. Queue kosong → bantu isi cache user aktif (meniru
                #    _cache_fill_worker instance asli, supaya cache tidak
                #    macet total selama freeze berlangsung).
                if not did_work:
                    now = time.time()
                    if now - last_fill_check >= 1.0:
                        last_fill_check = now
                        candidates = [
                            uid for uid, last_seen in list(instance._recent_active.items())
                            if uid not in instance._bio_queue_pending
                            and now - last_seen < 600  # 10 menit window, cukup longgar
                        ]
                        if candidates:
                            uid = candidates[0]
                            instance._bio_queue_pending.add(uid)
                            try:
                                await self._fetch_and_save(participant, instance, uid, None)
                                did_work = True
                            finally:
                                instance._bio_queue_pending.discard(uid)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                who = "pemantau" if participant == "pemantau" else f"Bengkel#{participant.index}"
                print(f"[BengkelJoin] Rotasi grup {self.chat_id} ({who}) error: {e}")

            # Jeda rate-limit SETELAH tiap fetch nyata, sebelum giliran
            # berikutnya — supaya tiap CLIENT individual tetap menjaga jarak
            # _BIO_QUEUE_DELAY antar requestnya sendiri.
            if did_work:
                await asyncio.sleep(_BIO_QUEUE_DELAY)
            else:
                # Tidak ada kerjaan sama sekali untuk siapapun saat ini.
                await asyncio.sleep(1.0)

        print(f"[BengkelJoin] Rotasi kerja gabungan berhenti grup {self.chat_id}.")

    async def _fetch_and_save(self, participant, instance, user_id: int, future) -> None:
        """
        Fetch bio user_id pakai Client milik `participant` ("pemantau" →
        instance.client asli; selain itu → Client Bengkel terkait), simpan
        ke bio_profiles dengan format & chat_id IDENTIK dengan yang ditulis
        MonitorInstance asli — consumer (bio.py dkk) tidak perlu tahu siapa
        yang menulis data ini.
        """
        from pyrogram.errors import FloodWait, PeerIdInvalid
        from pyrogram.raw import functions as raw_fns
        from pyrogram.raw.types import InputUser as _RawInputUser

        if participant == "pemantau":
            client = instance.client
            mark_fw = instance.mark_floodwait
            who = "pemantau"
        else:
            client = participant.client
            mark_fw = participant.mark_floodwait
            who = f"Bengkel#{participant.index}"

        bio: str | None = None
        try:
            try:
                peer = await client.resolve_peer(user_id)
            except (PeerIdInvalid, KeyError):
                peer = _RawInputUser(user_id=user_id, access_hash=0)

            full = await client.invoke(raw_fns.users.GetFullUser(id=peer))
            bio = getattr(full.full_user, "about", None) or ""

        except FloodWait as fw:
            mark_fw(fw.value + 1)
            print(f"[BengkelJoin] {who} FloodWait {fw.value}s uid={user_id} grup={self.chat_id} "
                  f"— giliran berikutnya akan skip peserta ini sampai reda.")
        except Exception as e:
            print(f"[BengkelJoin] {who} gagal fetch uid={user_id} grup={self.chat_id}: {e}")

        has_link: bool | None = None
        if bio is not None:
            from monitor_bot_reference import LINK_PATTERN, bio_col
            has_link = bool(LINK_PATTERN.search(bio))

            now = time.time()
            try:
                expires_at = instance._make_expires_at()
                await bio_col.update_one(
                    {"chat_id": instance.chat_id, "user_id": user_id},
                    {"$set": {
                        "chat_id": instance.chat_id,
                        "user_id": user_id,
                        "has_link": has_link,
                        "bio": bio,
                        "checked_at": now,
                        "updated_at": now,
                        "expires_at": expires_at,
                        "source": "workshop_join" if participant != "pemantau" else "workshop_join_pemantau",
                    }},
                    upsert=True,
                )
            except Exception as e:
                print(f"[BengkelJoin] Gagal simpan bio_profiles chat={instance.chat_id} uid={user_id}: {e}")

        if future is not None and not future.done():
            try:
                future.set_result(has_link)
            except Exception:
                pass


class WorkshopJoinPool:
    """
    Orkestrasi N BengkelBot — assign ke grup yang MonitorInstance-nya
    FloodWait, round-robin antar Bengkel yang idle, fallback ke
    workshop_pool standalone kalau semua Bengkel sedang dipakai.

    Join SELALU diverifikasi (get_chat_member) sebelum Bengkel dianggap
    boleh kerja. Begitu confirmed joined, grup itu masuk mode kerja
    gabungan lewat 1 _GroupWorkRotation per chat_id (mendukung >1 Bengkel
    per grup untuk kasus grup ramai).
    """

    def __init__(self, tokens: list[str]):
        self._bengkels: list[_BengkelBot] = [
            _BengkelBot(i, tok) for i, tok in enumerate(tokens)
        ]
        self._started = False
        self._start_lock = asyncio.Lock()
        self._assign_lock = asyncio.Lock()  # cegah race saat assign/release bersamaan
        self._pending_groups: list[int] = []  # FIFO chat_id yang nunggu slot Bengkel
        self._retrying_start: set[int] = set()  # index Bengkel yang sedang di-retry start()
        self._rotations: dict[int, _GroupWorkRotation] = {}  # chat_id -> rotation aktif
        self._main_app = None
        self._monitor_loop_task: asyncio.Task | None = None

    @property
    def size(self) -> int:
        return len(self._bengkels)

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def start_all(self, main_app) -> None:
        async with self._start_lock:
            if self._started:
                return
            self._main_app = main_app
            if self._bengkels:
                results = await asyncio.gather(
                    *[b.start(main_app) for b in self._bengkels], return_exceptions=True
                )
                ok = sum(1 for r in results if r is True)
                print(f"[BengkelJoin] Pool siap: {ok}/{len(self._bengkels)} Bengkel aktif.")
            self._started = True
            self._monitor_loop_task = asyncio.create_task(
                self._monitor_loop(), name="bengkel_join_monitor_loop"
            )

    async def stop_all(self) -> None:
        if self._monitor_loop_task and not self._monitor_loop_task.done():
            self._monitor_loop_task.cancel()
            try:
                await self._monitor_loop_task
            except asyncio.CancelledError:
                pass
        # Pastikan semua rotasi & Bengkel yang masih assigned keluar dulu dengan rapi
        for chat_id in list(self._rotations.keys()):
            await self._release_group(chat_id, reason="shutdown")
        await asyncio.gather(*[b.stop() for b in self._bengkels], return_exceptions=True)

    # ── Monitor loop: deteksi FloodWait & "aman" ──────────────────────────────

    async def _monitor_loop(self) -> None:
        """
        Poll semua MonitorInstance aktif setiap _POLL_INTERVAL_SECS:
          - Bengkel yang GAGAL start saat boot (FloodWait pada login) →
            coba start() lagi otomatis begitu waktu tunggunya lewat.
          - FloodWait & belum dalam mode kerja gabungan → minta bantuan
            (assign Bengkel pertama / antri).
          - SUDAH dalam mode kerja gabungan TAPI pemantau FloodWait LAGI →
            sinyal 1 Bengkel belum cukup → coba tambah 1 Bengkel lagi ke
            grup yang SAMA (grup ramai banget).
          - Sedang dibantu & sudah aman _SAFE_DURATION_SECS (pemantau DAN
            semua Bengkel grup itu tidak floodwait) → lepas semuanya.
          - Pemantau pulih SENDIRI sebelum kebagian slot Bengkel (masih di
            _pending_groups, belum frozen-assigned) → batalkan antrian,
            tidak ada gunanya menunggu Bengkel lagi.
        """
        from monitor_bot_reference import _active_instances

        while True:
            try:
                await asyncio.sleep(_POLL_INTERVAL_SECS)
                now_mono = time.monotonic()

                # ── -1. Retry otomatis untuk Bengkel yang GAGAL start saat
                #      boot (umumnya FloodWait pada auth.ImportBotAuthorization
                #      — lihat _BengkelBot.start()). Begitu waktu tunggu
                #      (busy_until) lewat, coba start() lagi di background.
                #      Tanpa ini, Bengkel yang gagal start tetap idle
                #      selamanya sampai bot di-restart manual. ───────────────
                for b in self._bengkels:
                    if (
                        not b._started
                        and not b.is_floodwait(now_mono)
                        and b.index not in self._retrying_start
                    ):
                        self._retrying_start.add(b.index)
                        asyncio.create_task(self._retry_start_bengkel(b))

                # ── 0. Batalkan antrian grup yang pemantaunya sudah pulih
                #      sendiri SEBELUM kebagian slot Bengkel ─────────────────
                if self._pending_groups:
                    still_pending = []
                    for chat_id in self._pending_groups:
                        inst = _active_instances.get(chat_id)
                        if inst is None or not inst.is_floodwait(now_mono):
                            print(f"[BengkelJoin] Grup {chat_id} sudah tidak FloodWait "
                                  f"sebelum kebagian slot Bengkel → antrian dibatalkan.")
                            continue
                        still_pending.append(chat_id)
                    self._pending_groups = still_pending

                for chat_id, instance in list(_active_instances.items()):
                    is_fw = instance.is_floodwait(now_mono)
                    rotation = self._rotations.get(chat_id)

                    if not instance.frozen:
                        if is_fw and chat_id not in self._pending_groups:
                            # Baru FloodWait & belum dalam mode kerja gabungan
                            # → minta bantuan Bengkel pertama sekarang.
                            asyncio.create_task(self._handle_floodwait(chat_id, instance))
                        continue

                    # instance.frozen == True → sedang dalam mode kerja gabungan
                    if rotation is None:
                        continue

                    if is_fw:
                        # Pemantau FloodWait LAGI selagi sudah dibantu →
                        # sinyal 1 (atau N) Bengkel belum cukup. Reset hitungan
                        # "aman" & coba tambah 1 Bengkel lagi ke grup ini.
                        for b in rotation.bengkels:
                            b._safe_since = None
                        asyncio.create_task(self._handle_floodwait(chat_id, instance))
                    else:
                        # Pemantau aman sekarang — tapi "aman keseluruhan"
                        # baru tercapai kalau SEMUA Bengkel grup ini juga
                        # tidak floodwait.
                        any_bengkel_fw = any(b.is_floodwait(now_mono) for b in rotation.bengkels)
                        if any_bengkel_fw:
                            for b in rotation.bengkels:
                                if b.is_floodwait(now_mono):
                                    b._safe_since = None
                            continue

                        ready_since = [
                            b._safe_since for b in rotation.bengkels if b._safe_since is not None
                        ]
                        if not rotation.bengkels:
                            continue
                        for b in rotation.bengkels:
                            if b._safe_since is None:
                                b._safe_since = now_mono
                        earliest = min(b._safe_since for b in rotation.bengkels)
                        if now_mono - earliest >= _SAFE_DURATION_SECS:
                            asyncio.create_task(self._release_group(chat_id, reason="aman"))

            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[BengkelJoin] monitor_loop error: {e}")
                await asyncio.sleep(5.0)

    # ── Assign / Verify-join / Release ──────────────────────────────────────

    async def _retry_start_bengkel(self, bengkel: "_BengkelBot") -> None:
        """
        Coba start() lagi untuk 1 Bengkel yang gagal start saat boot
        (umumnya kena FloodWait pada auth.ImportBotAuthorization).
        Dipanggil dari _monitor_loop setiap kali waktu tunggu (busy_until)
        Bengkel ini sudah lewat. Kalau gagal lagi (FloodWait lagi atau
        error lain), busy_until sudah di-update sendiri oleh start() —
        siklus poll berikutnya akan coba lagi otomatis tanpa intervensi.
        """
        try:
            ok = await bengkel.start(self._main_app)
            if ok:
                print(f"[BengkelJoin #{bengkel.index}] ✅ Retry start berhasil — Bengkel sekarang aktif.")
        finally:
            self._retrying_start.discard(bengkel.index)

    async def _handle_floodwait(self, chat_id: int, instance) -> None:
        """
        Grup ini FloodWait (baru atau lagi selagi sudah dibantu) — coba
        cari 1 Bengkel idle (round-robin, skip yang juga FloodWait) dan
        tambahkan ke grup. Kalau tidak ada slot, masuk antrian FIFO
        (fallback ke workshop_pool standalone sementara menunggu).
        """
        async with self._assign_lock:
            rotation = self._rotations.get(chat_id)
            already_working = rotation is not None and rotation.participant_count > 1

            # Kalau belum punya rotasi sama sekali tapi instance.frozen sudah
            # True dari proses assign lain yang lebih cepat → tidak usah lagi.
            if rotation is None and instance.frozen:
                return

            bengkel = self._pick_idle_bengkel()
            if bengkel is None:
                if not already_working and chat_id not in self._pending_groups:
                    self._pending_groups.append(chat_id)
                    print(f"[BengkelJoin] Grup {chat_id} FloodWait, semua Bengkel sibuk "
                          f"→ masuk antrian (fallback ke workshop_pool sementara).")
                elif already_working:
                    print(f"[BengkelJoin] Grup {chat_id} masih FloodWait & butuh Bengkel "
                          f"tambahan, tapi semua Bengkel sedang sibuk di grup lain.")
                return

            await self._assign_bengkel(bengkel, chat_id, instance)

    def _pick_idle_bengkel(self) -> "_BengkelBot | None":
        now = time.monotonic()
        candidates = [b for b in self._bengkels if b.is_idle and not b.is_floodwait(now) and b._started]
        if not candidates:
            return None
        return candidates[0]

    def _bengkel_for(self, chat_id: int) -> "_BengkelBot | None":
        """
        Bengkel PERTAMA yang assigned ke chat_id ini (kalau ada beberapa,
        karena grup ramai). Dipakai oleh /bjoin /bleave /bstatus (lihat
        plugins/commands/bengkel_test.py) — bukan dipakai oleh logika inti
        pool (yang sudah memakai self._rotations[chat_id].bengkels untuk
        mendukung >1 bengkel per grup).
        """
        for b in self._bengkels:
            if b.assigned_chat_id == chat_id:
                return b
        return None

    def bengkels_for(self, chat_id: int) -> list["_BengkelBot"]:
        """Semua Bengkel yang assigned ke chat_id ini (mendukung grup ramai)."""
        rotation = self._rotations.get(chat_id)
        return list(rotation.bengkels) if rotation else []

    async def _assign_bengkel(self, bengkel: "_BengkelBot", chat_id: int, instance) -> None:
        """
        Tarik bengkel masuk grup DAN verifikasi join sebelum membiarkannya
        ikut kerja. Kalau ini Bengkel PERTAMA untuk grup ini → bekukan
        instance asli & buat rotasi baru. Kalau grup ini SUDAH dalam mode
        kerja gabungan (rotasi sudah ada) → bengkel ini cuma ditambahkan
        sebagai peserta baru ke rotasi yang sudah berjalan (grup ramai).
        """
        if chat_id in self._pending_groups:
            self._pending_groups.remove(chat_id)

        rotation = self._rotations.get(chat_id)
        is_first_bengkel = rotation is None

        try:
            if is_first_bengkel:
                # Kosongkan antrian lama instance asli — jangan biarkan grup
                # menunggu proses antrian basi selama transisi.
                drained = instance.drain_queue()
                if drained:
                    print(f"[BengkelJoin] Grup {chat_id}: {drained} antrian lama dibuang "
                          f"sebelum Bengkel masuk.")

            # ── Tarik & VERIFIKASI join — bengkel TIDAK BOLEH kerja kalau
            #    tidak confirmed joined. ──────────────────────────────────
            bengkel.assigned_chat_id = chat_id
            joined = await self._attempt_join_and_verify(bengkel, chat_id)

            if not joined:
                print(f"[BengkelJoin] ❌ Bengkel #{bengkel.index} GAGAL dikonfirmasi join "
                      f"grup {chat_id} — dibatalkan, Bengkel kembali idle, grup balik antri.")
                bengkel.assigned_chat_id = None
                bengkel.joined = False
                if chat_id not in self._pending_groups and not (rotation and rotation.participant_count > 1):
                    self._pending_groups.append(chat_id)
                return

            bengkel.joined = True
            bengkel._safe_since = None

            if is_first_bengkel:
                instance.frozen = True
                rotation = _GroupWorkRotation(chat_id, instance)
                self._rotations[chat_id] = rotation

            rotation.add_bengkel(bengkel)
            rotation.start()

            print(f"[BengkelJoin] ✅ Bengkel #{bengkel.index} CONFIRMED JOIN grup {chat_id} "
                  f"— kerja gabungan ({rotation.participant_count} peserta, round-robin).")

        except Exception as e:
            print(f"[BengkelJoin] _assign_bengkel error chat={chat_id}: {e}")
            bengkel.assigned_chat_id = None
            bengkel.joined = False
            if is_first_bengkel:
                instance.frozen = False
                self._rotations.pop(chat_id, None)

    async def _attempt_join_and_verify(self, bengkel: "_BengkelBot", chat_id: int) -> bool:
        """
        Coba add_chat_members() lewat userbot, lalu VERIFIKASI status member
        Bengkel di grup tersebut via get_chat_member() — dengan retry singkat
        (Telegram kadang butuh sesaat sebelum status membership konsisten).

        Return True HANYA jika status member benar-benar terkonfirmasi.
        Tidak ada "anggap berhasil" tanpa konfirmasi — kalau add_chat_members
        gagal/di-skip ATAU verifikasi tidak pernah berhasil, return False.

        Catatan izin invite: kalau userbot bukan admin (termasuk karena hak
        admin/invite-nya DICABUT PAKSA di tengah jalan), add_chat_members
        di-skip SAMA SEKALI — tidak ada usaha invite yang dipaksakan, fungsi
        ini langsung lanjut ke verifikasi (yang otomatis gagal karena Bengkel
        memang belum/tidak masuk grup) dan return False dengan rapi.
        """
        from pyrogram.errors import UserNotParticipant, PeerIdInvalid

        try:
            from video_call import userbot as _ub, is_userbot_admin as _ub_is_admin
        except Exception as e:
            print(f"[BengkelJoin] Gagal import userbot module: {e} — join dibatalkan.")
            return False

        if _ub is None:
            print(f"[BengkelJoin] add_chat_members skip: userbot tidak aktif (bengkel=#{bengkel.index}).")
        elif not await _ub_is_admin(chat_id):
            # Termasuk kasus hak admin/invite userbot dicabut paksa — tidak
            # ada percobaan invite sama sekali, langsung lanjut ke verifikasi
            # (akan gagal dengan rapi) tanpa membuang API call untuk invite
            # yang sudah pasti ditolak.
            print(f"[BengkelJoin] add_chat_members skip: userbot bukan admin di chat={chat_id} "
                  f"(bengkel=#{bengkel.index}) — tidak ada usaha invite.")
        elif bengkel.user_id:
            try:
                await _ub.add_chat_members(chat_id, bengkel.user_id)
            except Exception as e:
                # Gagal invite (bengkel sudah ada di grup, hak invite ditolak
                # API, dll) — tidak fatal, lanjut ke verifikasi: kalau memang
                # sudah jadi member (mis. "sudah ada di grup") verifikasi akan
                # tetap berhasil; kalau tidak, verifikasi gagal dengan rapi.
                print(f"[BengkelJoin] add_chat_members gagal chat={chat_id} bengkel=#{bengkel.index}: {e}")

        # ── Verifikasi status member — WAJIB sebelum bengkel boleh kerja ──
        for attempt in range(1, _JOIN_VERIFY_RETRIES + 1):
            try:
                from pyrogram.enums import ChatMemberStatus
                member = await bengkel.client.get_chat_member(chat_id, bengkel.user_id)
                if member and member.status in (
                    ChatMemberStatus.MEMBER,
                    ChatMemberStatus.ADMINISTRATOR,
                    ChatMemberStatus.OWNER,
                    ChatMemberStatus.RESTRICTED,
                ):
                    return True
            except (UserNotParticipant, PeerIdInvalid):
                pass
            except Exception as e:
                print(f"[BengkelJoin] Verifikasi join gagal (attempt {attempt}/{_JOIN_VERIFY_RETRIES}) "
                      f"chat={chat_id} bengkel=#{bengkel.index}: {e}")

            if attempt < _JOIN_VERIFY_RETRIES:
                await asyncio.sleep(_JOIN_VERIFY_DELAY_SECS)

        return False

    async def _release_group(self, chat_id: int, reason: str = "aman") -> None:
        """
        Lepaskan SEMUA Bengkel yang sedang membantu grup ini: hentikan
        rotasi, unfreeze instance asli, leave_chat tiap Bengkel satu-persatu
        dengan jeda (_LEAVE_STAGGER_SECS).
        """
        rotation = self._rotations.pop(chat_id, None)
        if rotation is None:
            return

        await rotation.stop()

        from monitor_bot_reference import _active_instances
        instance = _active_instances.get(chat_id)
        if instance is not None:
            instance.frozen = False

        bengkels = list(rotation.bengkels)
        for bengkel in bengkels:
            await asyncio.sleep(_LEAVE_STAGGER_SECS)
            try:
                await bengkel.client.leave_chat(chat_id)
                print(f"[BengkelJoin] 👋 Bengkel #{bengkel.index} keluar dari grup {chat_id} "
                      f"(alasan: {reason}).")
            except Exception as e:
                print(f"[BengkelJoin] leave_chat gagal chat={chat_id} bengkel=#{bengkel.index}: {e}")

            bengkel.assigned_chat_id = None
            bengkel.joined = False
            bengkel._safe_since = None

        # Slot Bengkel ini sekarang bebas — kalau ada grup lain di antrian
        # FIFO, langsung proses sekarang juga.
        await self._drain_pending_queue()

    async def _drain_pending_queue(self) -> None:
        async with self._assign_lock:
            if not self._pending_groups:
                return
            from monitor_bot_reference import _active_instances
            bengkel = self._pick_idle_bengkel()
            if bengkel is None:
                return
            chat_id = self._pending_groups[0]
            instance = _active_instances.get(chat_id)
            if instance is None or not instance.is_floodwait():
                # Pemantau sudah pulih sendiri / instance hilang — batalkan,
                # tidak perlu assign Bengkel untuk grup yang tidak butuh lagi.
                self._pending_groups.pop(0)
                return
            self._pending_groups.pop(0)
            await self._assign_bengkel(bengkel, chat_id, instance)


def _collect_backup_tokens() -> list[str]:
    """Sama seperti core/workshop_pool.py — TOKEN_BACKUP1, 2, ... berurutan."""
    tokens: list[str] = []
    n = 1
    while True:
        val = os.environ.get(f"TOKEN_BACKUP{n}", "").strip()
        if not val:
            break
        tokens.append(val)
        n += 1
    return tokens


# ── Singleton pool ────────────────────────────────────────────────────────────
workshop_join_pool = WorkshopJoinPool(_collect_backup_tokens())


def is_pending_for_bengkel(chat_id: int) -> bool:
    """
    True jika grup ini SEDANG menunggu slot Bengkel (semua Bengkel sibuk di
    grup lain) — dipanggil dari bio.py SEBELUM force_check_user supaya bisa
    langsung lompat ke workshop_pool standalone tanpa nunggu timeout 30s
    dari _enqueue_bio_check yang pasti macet (tidak ada yang konsumsi queue
    selama status pending ini)."""
    return chat_id in workshop_join_pool._pending_groups


# ── Integrasi dengan bio.py ────────────────────────────────────────────────────
# TIDAK PERLU fungsi fallback terpisah di sini — bio.py SUDAH PUNYA rantai
# fallback sendiri (lihat plugins/filters/bio.py, blok "Step 2"):
#
#   1. force_check_user(chat_id, user_id)
#      → instance.check_and_save(force=True) → _enqueue_bio_check()
#      → masuk _bio_queue milik instance.
#        - Kalau instance TIDAK frozen (belum/tidak butuh Bengkel) →
#          _bio_worker instance sendiri yang proses seperti biasa.
#        - Kalau instance frozen DAN sudah ada rotasi aktif (≥1 Bengkel
#          confirmed joined) → _GroupWorkRotation (lihat _loop di atas) yang
#          konsumsi queue yang SAMA secara bergantian (pemantau ikut rotasi
#          juga, bukan cuma Bengkel) — PINJAM bukan salin — otomatis
#          terlayani tanpa kode tambahan di bio.py sama sekali.
#        - Kalau instance frozen TAPI belum ada Bengkel confirmed joined
#          (masih di _pending_groups, semua Bengkel sibuk, ATAU sedang
#          proses join+verifikasi) → queue menumpuk, tidak ada yang
#          konsumsi → _enqueue_bio_check timeout 4s → return None.
#   2. force_check_user mengembalikan None → bio.py lanjut ke
#      workshop_pool.check_and_save() (mode lama, standalone, tidak join
#      grup) sebagai penutup sementara.
#
# Jadi tidak ada perubahan apapun yang perlu dilakukan di bio.py untuk pool
# ini — keduanya (skema lama workshop_pool & skema baru workshop_join_pool)
# hidup berdampingan otomatis lewat mekanisme frozen + queue pinjaman di atas.
