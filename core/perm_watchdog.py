"""
core/perm_watchdog.py
───────────────────────────────────────────────────────────────────────────────
Watchdog izin ban/mute — pengecekan PROAKTIF & BERGILIR ke semua grup yang
dikenal bot, independen dari ada/tidaknya pesan masuk.

LATAR BELAKANG (kenapa ini perlu, padahal sudah ada check_bot_permissions):
  database.check_bot_permissions() bersifat REAKTIF — hanya terpicu saat ada
  pesan masuk ke grup, hasilnya di-cache 5 menit, dan FAIL-OPEN saat Telegram
  API error (supaya bot tidak berhenti total cuma karena network hiccup).

  Konsekuensinya: grup yang sepi pesan, atau yang baru saja kehilangan izin
  ban/mute tepat setelah cache terisi, bisa tetap mencoba eksekusi mute/ban
  selama hingga 5 menit (atau lebih, kalau API sempat fail-open) — itulah
  yang terlihat di log "Mute Gagal — Izin Bot Kurang": counter pelanggaran
  sudah naik, eksekusi API-nya yang gagal belakangan.

YANG DILAKUKAN WATCHDOG INI:
  Setiap siklus, iterasi SEMUA grup yang dikenal (config_db) satu per satu
  (dengan jeda antar grup, bukan sekaligus, supaya tidak membebani API), lalu:

    1. Grup tidak ditemukan lagi (bot dikick / grup dihapus / akses ditolak
       permanen) → block_and_remove_group(): hapus dari config_db + catat
       permanen ke blocked_groups supaya tidak diam-diam terdaftar lagi.

    2. Grup ditemukan tapi bot TIDAK punya can_delete_messages DAN
       can_restrict_members (kuasa hapus+ban/mute) →
       force_disable_group_moderation(): paksa toggle local/global/cas ke
       OFF di DATABASE (bukan cache sesaat) — sehingga seluruh proses yang
       berujung HAPUS PESAN / BAN / MUTE benar-benar berhenti untuk grup
       ini, persis seperti toggle dimatikan manual dari panel.

       Yang TETAP JALAN (tidak disentuh watchdog ini, lihat masing-masing
       modul untuk detail): pencatatan deteksi ubot (core/ubot_detect.py),
       pencatatan @mention (mention_cache/mention_global), dan passive
       learning Nexus AI (nexus/ai_core) — ketiganya hanya MENCATAT, tidak
       pernah menghapus pesan / membatasi user, jadi tetap aman dijalankan
       walau bot bukan admin.

    3. Grup ditemukan dan izin SUDAH PULIH, DAN grup ini sebelumnya
       dimatikan oleh watchdog (bukan oleh admin secara manual) →
       restore_group_moderation_if_forced(): nyalakan kembali local/global
       (cas TIDAK auto-restore — lihat alasan di database.py).

    4. Grup ditemukan (apapun status izin ban/mute-nya) →
       refresh_group_public_info(): sinkronkan username publik terbaru
       (atau invite link kalau grup privat) ke config_db, supaya panel
       "Grup Terdaftar" tidak menampilkan link basi yang sudah diganti/
       dihapus owner grup. Gagal generate invite link (tidak ada izin
       undang) di-skip diam-diam, tidak dianggap error.

DIJALANKAN sebagai 1 background task tunggal (lihat antigcast.py), mengikuti
pola task lain yang sudah ada (delete_worker, moderation_worker_loop, dst).
"""

import asyncio
import os

PERM_WATCHDOG_INTERVAL = int(os.environ.get("PERM_WATCHDOG_INTERVAL", 3600))  # detik, default 1 jam
_PER_GROUP_DELAY        = float(os.environ.get("PERM_WATCHDOG_DELAY", 1.0))   # jeda antar grup (detik)

# Error yang berarti bot sudah tidak punya akses ke grup ini sama sekali
# (dikick, grup dihapus, dll) — bukan sekadar kehilangan izin admin.
_GROUP_GONE_ERRORS = (
    "UserNotParticipant", "ChannelPrivate", "ChatForbidden",
    "ChatIdInvalid", "PeerIdInvalid", "ChannelInvalid",
)


async def _check_one_group(client, chat_id: int) -> None:
    from database import (
        force_disable_group_moderation, restore_group_moderation_if_forced,
        block_and_remove_group, refresh_group_public_info,
    )

    try:
        me = client.me
        member = await client.get_chat_member(chat_id, me.id)
    except Exception as e:
        err_cls = type(e).__name__
        if err_cls in _GROUP_GONE_ERRORS:
            await block_and_remove_group(chat_id, reason=err_cls)
        else:
            # Error lain (FloodWait, jaringan, dsb.) — jangan ambil keputusan
            # destruktif berdasarkan kegagalan sementara. Coba lagi siklus
            # berikutnya.
            print(f"[PermWatchdog] ⚠️  Grup {chat_id}: gagal cek ({err_cls}: {e}) — dilewati siklus ini.")
        return

    privs        = getattr(member, "privileges", None)
    can_del      = bool(getattr(privs, "can_delete_messages",  False)) if privs else False
    can_restrict = bool(getattr(privs, "can_restrict_members", False)) if privs else False
    has_perms    = can_del and can_restrict

    if not has_perms:
        await force_disable_group_moderation(chat_id)
    else:
        await restore_group_moderation_if_forced(chat_id)

    # Grup masih aktif & terakses — sinkronkan username/invite link terbaru
    # supaya panel "Grup Terdaftar" tidak menampilkan info basi. Dijalankan
    # terlepas status izin ban/mute (info publik grup independen dari itu).
    try:
        await refresh_group_public_info(client, chat_id)
    except Exception as e:
        print(f"[PermWatchdog] ⚠️  Grup {chat_id}: gagal refresh info publik: {e}")


async def perm_watchdog_loop(client) -> None:
    """
    Loop tunggal: tunggu client siap, lalu setiap PERM_WATCHDOG_INTERVAL
    detik, iterasi seluruh grup yang dikenal secara bergilir (dengan jeda
    kecil antar grup) untuk memverifikasi kuasa ban/mute bot.
    """
    from database import get_all_known_group_ids

    for _ in range(60):
        if getattr(client, "is_connected", False):
            break
        await asyncio.sleep(1.0)

    print(f"[PermWatchdog] ✅ Watchdog izin ban/mute aktif "
          f"(siklus tiap {PERM_WATCHDOG_INTERVAL}s).", flush=True)

    while True:
        try:
            group_ids = await get_all_known_group_ids()
        except Exception as e:
            print(f"[PermWatchdog] ⚠️  Gagal ambil daftar grup: {e}")
            await asyncio.sleep(PERM_WATCHDOG_INTERVAL)
            continue

        for chat_id in group_ids:
            try:
                await _check_one_group(client, chat_id)
            except asyncio.CancelledError:
                return
            except Exception as e:
                print(f"[PermWatchdog] ❌ Error tak terduga grup {chat_id}: {e}")
            await asyncio.sleep(_PER_GROUP_DELAY)

        await asyncio.sleep(PERM_WATCHDOG_INTERVAL)
