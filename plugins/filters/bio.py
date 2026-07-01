"""
plugins/filters/bio.py
──────────────────────
Filter deteksi link di bio Telegram user — BOT UTAMA.
Berjalan di group=1 (sebelum antispam.py di group=2).

ARSITEKTUR (Database-driven + TTL, per-grup):
  Bot utama TIDAK langsung fetch bio dari Telegram API.
  Tiap grup punya bot pemantau sendiri (token dari admin).
  Bot pemantau masing-masing grup menulis hasil scan ke bio_profiles
  dengan field chat_id sebagai pemisah antar grup.
  Data bio ber-TTL 5 menit — MongoDB hapus otomatis setelah expires_at.

  ALUR saat ada pesan masuk di grupA:
    1. bio_filter dipanggil → cek konfigurasi bio_check aktif?
    2. Query bio_profiles { chat_id: grupA, user_id: X }
       ← data ini ditulis oleh bot pemantau khusus grupA
    3a. has_link=True  → hapus pesan + hukuman
    3b. has_link=False → abaikan (biarkan lewat)
    3c. None (data belum ada / sudah expired TTL) →
          lempar user_id ke bot pemantau via force_check_user()
          → bot pemantau fetch bio → simpan ke DB (TTL 5 menit baru)
          → pakai hasilnya

  ALUR saat ada TYPING dari user di grupA:
    1. raw handler bot utama terima UpdateUserTyping
    2. Cek bio_profiles → ada data? pakai langsung (tidak ganggu API)
    3. Tidak ada data? → panggil force_check_user() ke bot pemantau
       → bot pemantau fetch bio → simpan DB → catat hasil di memory cache

DATA FLOW PER-GRUP:
  Bot pemantau grupA hanya tulis data chat_id=grupA.
  Bot utama hanya baca data chat_id=grupA saat ada event di grupA.
  Data hilang otomatis setelah 5 menit (MongoDB TTL index).
  Saat user aktif → data selalu fresh karena di-refresh setiap aksi.

FALLBACK AMAN:
  Jika force_check_user gagal (instance tidak aktif / error) → lewatkan.
  Tidak ada false positive.
"""

import os
import asyncio
import time
import html
from datetime import datetime
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode
from pyrogram.raw import types as raw_types

from database import (
    is_admin, delete_queue, get_config,
    db, TZ_WIB,
    mark_message_handled, is_message_handled, insert_group_action_log,
    check_bot_permissions,
)
from core.punishment import check_and_punish
from core.violation_types import VIOLATION_BIO_LINK, format_violation_header

free_col    = db["free_per_group"]
bio_col     = db["bio_profiles"]    # Ditulis oleh bot pemantau masing-masing grup
LOG_CHANNEL = int(os.environ.get("LOG_CHANNEL", 0))

# ── In-memory cache: hindari query DB berulang untuk user+grup yang sama ──────
# Key: (chat_id, user_id) — per-grup karena data bio bersifat per-grup
# Value: (has_link: bool, cache_ts: float)
# Cache ini hanya dipakai saat data BARU saja di-fetch (< 5 menit).
# Setelah TTL DB habis, cache ini juga akan expired (TTL sama = 300 detik).
_mem_cache: dict[tuple[int, int], tuple[bool, float]] = {}
# BUG 3 FIX: TTL 60 detik — sinkron dengan BIO_TTL_SECS (monitor_bot) &
# _BIO_CACHE_TTL (userbot). Semua layer cache hancur dalam waktu yang sama.
# Satu-satunya sumber kebenaran TTL bio: BIO_TTL_SECS di .env.
# Default 300 — konsisten dengan monitor_bot_reference.py dan video_call.py.
_MEM_CACHE_TTL = float(os.environ.get("BIO_TTL_SECS", 300))

# ── VIP bio cache: apakah user ini VIP berdasarkan teks bio-nya ───────────────
# Key: (chat_id, user_id), Value: (is_vip: bool, cache_ts: float)
# TTL menggunakan nilai yang SAMA dengan _MEM_CACHE_TTL agar kedua cache
# expired bersamaan — pengecekan VIP selalu berjalan 1x bersama cek bio link.
# Tidak ada API tambahan: data bio sudah di-fetch oleh bot pemantau, kita
# hanya membaca field "bio" dari dokumen yang sama di bio_profiles.
_VIP_BIO_CACHE_TTL = _MEM_CACHE_TTL  # ikut BIO_TTL_SECS — 1 env var mengatur semua
_vip_bio_cache: dict[tuple[int, int], tuple[bool, float]] = {}

# ── Throttle typing handler bot utama ─────────────────────────────────────────
# Untuk mencegah force_check_user dipanggil terlalu sering dari sisi bot utama.
# Key: (chat_id, user_id), Value: timestamp terakhir trigger
_typing_trigger_ts: dict[tuple[int, int], float] = {}
_TYPING_TRIGGER_COOLDOWN = _MEM_CACHE_TTL  # ikut BIO_TTL_SECS — konsisten dgn cache lain


async def _query_bio_for_group(chat_id: int, user_id: int) -> bool | None:
    """
    Query hasil bio dari DB untuk pasangan (chat_id, user_id).
    Data ini KHUSUS grup ini — ditulis oleh bot pemantau grup ini.
    Data ber-TTL 5 menit — jika expired, MongoDB sudah hapus → return None.

    Return:
      True  → ada link di bio (data masih valid di DB)
      False → tidak ada link di bio (data masih valid di DB)
      None  → belum ada data / sudah expired → perlu force_check_user
    """
    now = time.monotonic()
    key = (chat_id, user_id)

    # Memory cache dulu — untuk menghindari query DB berulang dalam 5 menit
    cached = _mem_cache.get(key)
    if cached:
        has_link, cache_ts = cached
        if now - cache_ts < _MEM_CACHE_TTL:
            return has_link
        else:
            # Cache expired → hapus
            del _mem_cache[key]

    # Query DB
    try:
        doc = await bio_col.find_one({"chat_id": chat_id, "user_id": user_id})
    except Exception as e:
        print(f"[Bio-Filter] Gagal query bio chat={chat_id} uid={user_id}: {e}")
        return None

    if not doc:
        # Belum ada data atau sudah dihapus TTL MongoDB → perlu fresh check
        return None

    has_link = doc.get("has_link", False)

    # Update memory cache
    _mem_cache[key] = (has_link, now)
    return has_link


def _update_mem_cache(chat_id: int, user_id: int, has_link: bool) -> None:
    """Update memory cache setelah force_check_user berhasil."""
    _mem_cache[(chat_id, user_id)] = (has_link, time.monotonic())


async def _check_vip_bio(chat_id: int, user_id: int, vip_text: str) -> bool:
    """
    Cek apakah user ini VIP berdasarkan teks di bionya.

    Alur:
      1. Cek _vip_bio_cache dulu (TTL = BIO_TTL_SECS, sama dengan bio link cache)
      2. Jika cache miss → baca field "bio" dari dokumen bio_profiles yang
         SUDAH ada (ditulis bot pemantau saat cek bio link — TIDAK ada API
         tambahan untuk kasus ini)
      3. Jika dokumen BELUM ADA SAMA SEKALI (user baru pertama kali terlihat,
         atau cache TTL bio_profiles habis) → PAKSA fetch fresh via bot
         pemantau (force_check_user), SAMA seperti yang dilakukan
         _query_bio_for_group() untuk has_link. Tanpa ini, VIP check akan
         selalu return False palsu pada pesan PERTAMA seorang user — dan
         karena bio_filter() menjalankan cek VIP SEBELUM cek link, giliran
         _query_bio_for_group() berikutnya baru memicu fetch (sudah
         terlambat untuk pesan yang sama) → user dengan teks VIP DAN link
         sekaligus di bio bisa salah kena hapus/hukum di pesan pertamanya,
         padahal seharusnya teks VIP mengalahkan deteksi link.
      4. Cek apakah vip_text ada di bio (case-insensitive)
      5. Simpan ke cache, return hasil

    Dipanggil bersamaan dengan cek bio link di bio_filter() —
    pada user yang sudah punya data di DB, ini tetap 1 jalan / 0 API
    tambahan seperti sebelumnya; hanya user yang BENAR-BENAR belum punya
    data sama sekali yang memicu 1x fetch (dan fetch itu juga dipakai
    ulang oleh _query_bio_for_group() lewat memory cache, tidak dobel).

    Return:
      True  → user VIP (bio mengandung vip_text)
      False → user bukan VIP
    """
    now = time.monotonic()
    key = (chat_id, user_id)

    # Cek cache dulu
    cached = _vip_bio_cache.get(key)
    if cached:
        is_vip, cache_ts = cached
        if now - cache_ts < _VIP_BIO_CACHE_TTL:
            return is_vip
        del _vip_bio_cache[key]

    # Baca bio dari dokumen yang sudah ada di DB (bukan fetch baru)
    try:
        doc = await bio_col.find_one({"chat_id": chat_id, "user_id": user_id})
    except Exception as e:
        print(f"[Bio-VIP] Gagal query bio_profiles chat={chat_id} uid={user_id}: {e}")
        return False

    if not doc:
        # Data belum ada (user baru / TTL bio_profiles habis) — PAKSA fetch
        # fresh sekarang, sama seperti _query_bio_for_group() melakukan untuk
        # has_link. Tanpa ini, pesan pertama user akan selalu dianggap
        # "bukan VIP" walau teksnya sudah ada di bio, karena belum pernah
        # ada yang fetch bio-nya sama sekali.
        try:
            from monitor_bot_reference import force_check_user
            await force_check_user(chat_id, user_id)
        except Exception as e:
            print(f"[Bio-VIP] force_check_user gagal chat={chat_id} uid={user_id}: {e}")
            return False

        try:
            doc = await bio_col.find_one({"chat_id": chat_id, "user_id": user_id})
        except Exception as e:
            print(f"[Bio-VIP] Gagal re-query bio_profiles chat={chat_id} uid={user_id}: {e}")
            return False

        if not doc:
            # Bot pemantau tidak aktif / gagal total — tidak ada data sama
            # sekali. Tidak ada yang bisa dipastikan → anggap belum VIP,
            # akan dicoba lagi di pesan berikutnya.
            return False

    bio_text = doc.get("bio", "") or ""
    is_vip   = vip_text.lower() in bio_text.lower()

    # Simpan ke cache
    _vip_bio_cache[key] = (is_vip, now)

    if is_vip:
        print(f"[Bio-VIP] uid={user_id} chat={chat_id} → VIP (teks ditemukan di bio)")

    return is_vip


# ── Bio Admin Wajib (NewsCore) ──────────────────────────────────────────────
# Throttle ringan khusus pengecekan ini, terpisah dari _mem_cache (yang
# menyimpan has_link), agar tidak ada interaksi tak sengaja antar fitur.
_ns_bio_check_ts: dict[tuple[int, int], float] = {}
_NS_BIO_CHECK_COOLDOWN = _MEM_CACHE_TTL  # ikut TTL bio yang sama

# ── Cache: apakah salah satu fitur bio aktif di grup ini ─────────────────────
# Agar tidak query DB tiap pesan/typing hanya untuk cek "apakah perlu prefetch".
# TTL = sama dengan MEM_CACHE_TTL sehingga menyesuaikan jika admin toggle fitur.
_bio_feature_active_cache: dict[int, tuple[bool, float]] = {}


async def _any_bio_feature_active(chat_id: int) -> bool:
    """
    Return True jika setidaknya SATU fitur yang membutuhkan data bio aktif
    di grup ini: bio_check (link detector), Security OS, atau NewsCore.

    Dipakai oleh typing handler agar prefetch cache bio tetap berjalan
    meski bio_check mati — selama Security OS atau NewsCore masih butuh data bio.
    """
    now = time.monotonic()
    cached = _bio_feature_active_cache.get(chat_id)
    if cached and (now - cached[1]) < _MEM_CACHE_TTL:
        return cached[0]

    result = False
    try:
        cfg = await get_config(chat_id)
        if cfg.get("bio_check"):
            result = True
        if not result:
            ns_cfg = await db["newscore_config"].find_one({"chat_id": chat_id}) or {}
            if ns_cfg.get("enabled") and ns_cfg.get("bio_admin_required", True) and ns_cfg.get("bio_admin_text", "").strip():
                result = True
        if not result:
            secos_doc = await db["security_os"].find_one({"chat_id": chat_id}) or {}
            if secos_doc.get("enabled"):
                result = True
    except Exception as e:
        print(f"[Bio] _any_bio_feature_active error chat={chat_id}: {e}")
        result = False

    _bio_feature_active_cache[chat_id] = (result, now)
    return result


def _invalidate_bio_feature_cache(chat_id: int) -> None:
    """Hapus cache _any_bio_feature_active saat toggle berubah."""
    _bio_feature_active_cache.pop(chat_id, None)


async def _check_ns_admin_bio(client: Client, chat_id: int, user_id: int) -> None:
    """
    Dipicu setiap ada pesan dari user di grup (group=1, sebelum skip-admin).
    Jika user adalah admin NewsCore aktif → pastikan bio sudah dicek fresh,
    baca hasil admin_bio_ok, lalu unadmin via core.ns_bio_guard jika perlu.

    Tidak mempengaruhi pesan yang sedang diproses sama sekali (tidak hapus,
    tidak mark_message_handled) — berjalan independen sebagai background task.
    """
    try:
        from database import ns_get_config, ns_get_current_admins
        ns_cfg = await ns_get_config(chat_id)
        if not ns_cfg.get("enabled"):
            return

        ns_admins = await ns_get_current_admins(chat_id)
        if user_id not in {a["user_id"] for a in ns_admins}:
            return  # bukan admin NewsCore — tidak relevan

        # Throttle agar tidak query/trigger berulang dalam waktu singkat
        now = time.monotonic()
        key = (chat_id, user_id)
        last = _ns_bio_check_ts.get(key, 0)
        if now - last < _NS_BIO_CHECK_COOLDOWN:
            return
        _ns_bio_check_ts[key] = now

        from monitor_bot_reference import query_admin_bio_ok, force_check_user
        admin_bio_ok = await query_admin_bio_ok(chat_id, user_id)
        if admin_bio_ok is None:
            # Data belum ada / expired → paksa bot pemantau cek fresh
            # (force_check_user juga otomatis menulis admin_bio_ok)
            await force_check_user(chat_id, user_id)
            admin_bio_ok = await query_admin_bio_ok(chat_id, user_id)

        if admin_bio_ok is False:
            from core.ns_bio_guard import enforce_admin_bio
            await enforce_admin_bio(client, chat_id, user_id, admin_bio_ok)
    except Exception as e:
        print(f"[NS-BioGuard] _check_ns_admin_bio error chat={chat_id} uid={user_id}: {e}")


# ── Handler pesan masuk ────────────────────────────────────────────────────────

@Client.on_message((filters.group | filters.forum) & ~filters.service, group=1)
async def bio_filter(client: Client, message: Message):
    if not message.from_user or message.from_user.is_bot:
        return

    cid = message.chat.id
    uid = message.from_user.id
    mid = message.id

    if is_message_handled(cid, mid):
        return

    # ── Cek izin bot: HARUS punya delete_messages DAN restrict_members ───────
    if not await check_bot_permissions(client, cid):
        return

    cfg = await get_config(cid)

    # ── Bio Admin Wajib (NewsCore) — dicek SEBELUM skip-admin di bawah,
    # karena justru admin (yang diangkat NewsCore) yang perlu diperiksa di sini.
    # Tidak menghapus pesan ini sama sekali — hanya trigger unadmin jika perlu.
    asyncio.create_task(_check_ns_admin_bio(client, cid, uid))

    if not cfg["bio_check"]:
        return

    if await is_admin(client, cid, uid):
        return

    if await free_col.find_one({"user_id": uid, "chat_id": cid}):
        return

    # ── VIP Bio: cek bersamaan dengan bio link (1 jalan, 0 API tambahan) ──────
    # vip_text hanya dipakai jika bio_check aktif (sudah dicek di atas).
    # Cek VIP SEBELUM step 1 agar user VIP tidak terkena penghapusan
    # walau has_link=True sekalipun.
    vip_text = (cfg.get("bio_vip_text") or "").strip()
    if vip_text:
        is_vip = await _check_vip_bio(cid, uid, vip_text)
        if is_vip:
            # User baru terdeteksi VIP lewat teks bio — daftarkan ke
            # free_per_group (source="bio_vip") + buka mute jika ada.
            # Lihat core/vip_bio_guard.py untuk detail alur lengkapnya.
            from core.vip_bio_guard import maybe_enter_vip_bio
            asyncio.create_task(maybe_enter_vip_bio(client, cid, uid))
            return  # User VIP → bebas dari seluruh pengecekan bio

    # ── Step 1: Cek data dari DB (data bot pemantau grup ini) ─────────────────
    has_link = await _query_bio_for_group(cid, uid)

    # ── Step 2: Tidak ada data → paksa bot pemantau cek langsung ─────────────
    if has_link is None:
        # Cek dulu: kalau grup ini sedang menunggu slot Bengkel (semua
        # Bengkel sibuk di grup lain), force_check_user PASTI timeout 30s
        # (queue instance ini menumpuk, tidak ada yang konsumsi selagi
        # pending). Lompat langsung ke workshop_pool standalone supaya tidak
        # bikin pesan user menunggu lama tanpa alasan.
        skip_force_check = False
        try:
            from core.workshop_join_pool import is_pending_for_bengkel
            skip_force_check = is_pending_for_bengkel(cid)
        except Exception:
            pass

        if not skip_force_check:
            try:
                from monitor_bot_reference import force_check_user
                has_link = await force_check_user(cid, uid)
                if has_link is not None:
                    _update_mem_cache(cid, uid, has_link)
            except Exception:
                pass
        # FALLBACK BENGKEL: MonitorInstance grup ini tidak aktif/gagal
        # (belum setup token monitor, atau sedang FloodWait) → coba pool
        # token backup (workshop_pool). Hasilnya ditulis ke bio_profiles
        # dengan field sama persis, jadi transparan untuk semua consumer.
        if has_link is None:
            try:
                from core.workshop_pool import workshop_pool
                has_link = await workshop_pool.check_and_save(cid, uid)
                if has_link is not None:
                    _update_mem_cache(cid, uid, has_link)
            except Exception as e:
                print(f"[Bio-Filter] Bengkel fallback gagal chat={cid} uid={uid}: {e}")
        # BUG 4+5 FIX: Jika bot pemantau tidak bisa cek (user tidak dikenali/bio kosong)
        # → anggap tidak ada link (bio kosong = no link)
        # → biarkan pesan lewat, jangan hapus tanpa data yang valid
        if has_link is None:
            has_link = False   # bio kosong / tidak tersedia → dianggap no link
            print(
                f"[Bio-Filter] uid={uid} chat={cid}: "
                "bio tidak tersedia dari bot pemantau — dianggap no link, pesan dilewat"
            )
            return  # Tidak ada link → pesan aman, lanjut

    # ── Step 3: Eksekusi jika ada link ────────────────────────────────────────
    if has_link:
        mark_message_handled(cid, mid)
        await delete_queue.put((cid, [mid]))
        asyncio.create_task(_log_bio_deletion(client, message))
        try:
            await insert_group_action_log(
                cid, "HAPUS",
                "Link ditemukan di profil bio",
                uid,
                message.from_user.first_name or str(uid),
                (message.text or message.caption or "")[:100],
                jenis=VIOLATION_BIO_LINK,
            )
        except Exception:
            pass
        asyncio.create_task(
            check_and_punish(client, message, "link di bio profil", "")
        )


# ── Handler typing dari bot utama ─────────────────────────────────────────────
# Bot utama juga pasang raw handler typing agar:
#   - User baru mengetik tapi belum pernah kirim pesan → bot pemantau belum
#     scan → data tidak ada di DB → bot utama trigger force_check_user
#   - Setelah data expired TTL (5 menit tidak aktif) → user typing → fresh check

@Client.on_raw_update(group=1)
async def bio_typing_handler(client: Client, update, users, chats):
    """
    Handler typing di bot utama.
    Tujuan: jika user typing tapi data bio tidak ada di DB (belum pernah dicek
    atau sudah expired TTL), bot utama trigger force_check_user ke bot pemantau.
    Hasilnya dicatat di memory cache sehingga saat pesan masuk, bio_filter
    sudah punya data tanpa perlu force_check_user lagi.
    """
    try:
        if not isinstance(update, raw_types.UpdateUserTyping):
            return

        user_id = getattr(update, "user_id", None)
        if not user_id or not isinstance(user_id, int) or user_id <= 0:
            return

        # UpdateUserTyping tidak selalu membawa chat_id secara langsung.
        # Kita tidak tahu grup mana yang di-typing → skip tanpa chat_id.
        # Handler ini bergantung pada bot pemantau yang sudah tahu konteks grupnya.
        # Bot utama hanya bisa trigger berdasarkan pesan masuk (bio_filter di atas).
        # Raw handler typing di bot utama untuk future use / supergroup context.
        peer = getattr(update, "peer", None)
        if peer is None:
            return

        # Ambil chat_id dari peer jika tersedia
        chat_id = None
        if hasattr(peer, "chat_id"):
            chat_id = -peer.chat_id
        elif hasattr(peer, "channel_id"):
            chat_id = int(f"-100{peer.channel_id}")

        if not chat_id:
            return

        # Throttle: jangan trigger terlalu sering dari bot utama
        now = time.monotonic()
        key = (chat_id, user_id)
        last_trigger = _typing_trigger_ts.get(key, 0)
        if now - last_trigger < _TYPING_TRIGGER_COOLDOWN:
            return
        _typing_trigger_ts[key] = now

        # ── Bio Admin Wajib (NewsCore) — independen dari toggle bio_check
        # member di bawah, karena ini soal kewajiban admin, bukan filter link.
        asyncio.create_task(_check_ns_admin_bio(client, chat_id, user_id))

        # ── Cek izin bot: HARUS punya delete_messages DAN restrict_members ───
        # Khusus untuk jalur filter link-di-bio (hapus pesan). Jika tidak ada
        # → skip grup ini sepenuhnya untuk jalur ini (tidak query config/VIP/
        # bio sama sekali, tidak trigger force_check_user).
        if not await check_bot_permissions(client, chat_id):
            return

        # ── Cek apakah SETIDAKNYA SATU fitur yang butuh data bio aktif ───────
        # bio_check = filter link bio biasa
        # Security OS / NewsCore = fitur lain yang juga butuh data bio di DB
        # Jika semua OFF → tidak ada gunanya prefetch, langsung return.
        if not await _any_bio_feature_active(chat_id):
            return

        cfg = await get_config(chat_id)

        # ── VIP Bio: hanya relevan jika bio_check aktif ───────────────────────
        if cfg.get("bio_check"):
            vip_text = (cfg.get("bio_vip_text") or "").strip()
            if vip_text:
                is_vip = await _check_vip_bio(chat_id, user_id, vip_text)
                if is_vip:
                    from core.vip_bio_guard import maybe_enter_vip_bio
                    asyncio.create_task(maybe_enter_vip_bio(client, chat_id, user_id))
                    return  # User VIP → tidak perlu trigger bot pemantau

        # ── Prefetch cache bio jika data tidak ada / expired ─────────────────
        # Berjalan selama SALAH SATU fitur yang butuh data bio aktif,
        # bukan hanya saat bio_check aktif.
        has_link = await _query_bio_for_group(chat_id, user_id)
        if has_link is not None:
            return  # Data sudah ada dan masih valid

        # Data tidak ada / expired → trigger bot pemantau untuk isi cache
        try:
            from monitor_bot_reference import force_check_user
            result = await force_check_user(chat_id, user_id)
            if result is not None:
                _update_mem_cache(chat_id, user_id, result)
                print(
                    f"[Bio-Typing] uid={user_id} chat={chat_id} "
                    f"→ pre-fetch bio, has_link={result}"
                )
        except Exception:
            result = None
        # FALLBACK BENGKEL: sama seperti bio_filter — kalau MonitorInstance
        # grup ini tidak aktif/gagal, coba pool token backup.
        if result is None:
            try:
                from core.workshop_pool import workshop_pool
                result = await workshop_pool.check_and_save(chat_id, user_id)
                if result is not None:
                    _update_mem_cache(chat_id, user_id, result)
                    print(
                        f"[Bio-Typing] uid={user_id} chat={chat_id} "
                        f"→ pre-fetch bio via Bengkel, has_link={result}"
                    )
            except Exception as e:
                print(f"[Bio-Typing] Bengkel fallback gagal chat={chat_id} uid={user_id}: {e}")

    except Exception as e:
        print(f"[Bio-Typing] Error raw handler: {e}")


async def _log_bio_deletion(client: Client, message: Message):
    if not LOG_CHANNEL:
        return

    from plugins.commands.log import _user_line, _fmt_waktu

    uid          = message.from_user.id
    cid          = message.chat.id
    user_mention = _user_line(uid, message.from_user.first_name)
    content      = (message.text or message.caption or "").strip()

    # Ambil bio dari DB (data khusus grup ini)
    try:
        doc = await bio_col.find_one({"chat_id": cid, "user_id": uid})
        bio_snippet = doc.get("bio", "(tidak diketahui)")[:150] if doc else "(tidak diketahui)"
    except Exception:
        bio_snippet = "(tidak diketahui)"

    log_text = (
        f"<b>❖ {format_violation_header(VIOLATION_BIO_LINK)} ❖</b>\n"
        f"◈ <b>User:</b> {user_mention}\n"
        f"◈ <b>Grup:</b> {html.escape(message.chat.title)} (<code>{cid}</code>)\n"
        f"◈ <b>Waktu:</b> {_fmt_waktu()}\n"
        f"◈ <b>Keterangan:</b> Bio profil Telegram user mengandung link\n"
        f"◈ <b>Kebijakan:</b> Grup ini tidak mengizinkan member dengan bio berisi link\n"
        f"◈ <b>Bio terdeteksi:</b> <code>{html.escape(str(bio_snippet))}</code>\n\n"
        f"📨 <b>Pesan terakhir:</b>\n<code>{html.escape(content[:400])}</code>"
    )
    try:
        await client.send_message(
            LOG_CHANNEL, log_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception as e:
        print(f"[BIO LOG ERROR] {e}")
