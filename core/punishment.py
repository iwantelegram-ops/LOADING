"""
core/punishment.py
──────────────────
Sistem Hukuman Terpusat — berlaku untuk SEMUA jenis pelanggaran spam.

CARA KERJA:
  • Setiap deteksi spam (apapun jenisnya) memanggil check_and_punish().
  • Setelah 10 pelanggaran berturut-turut di grup yang sama → mute 5 menit.
  • Jika user masih spam setelah muted lagi → durasi 2× lipat (10, 20, 40, ... menit).
  • Setelah mute habis: hitungan spam TETAP di angka 10 (tidak direset ke 0),
    sehingga 1 pelanggaran berikutnya langsung memicu mute level berikutnya.
    Restart bot (Termux mati/hidup) tidak mereset hitungan karena data tersimpan
    persisten di database.
  • Pesan bersih (lolos semua filter, group=10) → reset hitungan + level hukuman.
  • Berlaku per user per grup — tidak campur antar grup.
  • Gcast: hanya grup yang mengaktifkan global detection yang menghitung punishment.

API Publik:
  check_and_punish(client, message, spam_type, konten) → bool
    Tambah hitungan. Terapkan mute jika ambang tercapai.
    Return True jika mute diterapkan, False jika belum/sudah muted.
"""

import os
import asyncio
import time
import html
from datetime import datetime, timedelta, timezone

from pyrogram.enums import ParseMode

from database import (
    get_local_mute, increment_local_spam, apply_local_mute,
    revert_failed_local_mute, auto_delete_reply, insert_group_action_log, TZ_WIB,
)
from core.group_notify import send_group_notice
from core.moderation_queue import queue_mute
from core.violation_types import (
    VIOLATION_MUTE_ESKALASI, VIOLATION_MUTE_GAGAL, format_violation_header,
)

LOG_CHANNEL         = int(os.environ.get("LOG_CHANNEL", 0))
SPAM_MUTE_THRESHOLD = 10   # Jumlah pelanggaran sebelum mute diterapkan


async def check_and_punish(
    client,
    message,
    spam_type: str,
    konten: str = "",
) -> bool:
    """
    Dipanggil oleh setiap filter setelah mendeteksi spam.
    Menambah hitungan pelanggaran berturut-turut per user per grup.
    Jika mencapai ambang (10) → antrikan mute (lihat core/moderation_queue.py
    — aksi mute dieksekusi oleh worker terpisah, BUKAN langsung di sini, agar
    banyak mute yang terjadi bersamaan saat raid tidak ditembak serentak ke
    Telegram API dan memicu FloodWait).

    `spam_type` (jenis pemicu, mis. "filter kata global") HANYA dipakai untuk
    pesan singkat di grup ("...di-mute karena X berulang") — TIDAK lagi
    ditampilkan di LOG_CHANNEL/panel sebagai bagian dari label utama. Label
    log mute eskalasi SELALU generik ("🔇 Mute Eskalasi (10× Berulang)"),
    sesuai desain: yang penting tercatat adalah AMBANG 10× tercapai, bukan
    jenis pemicu spesifiknya (lihat VIOLATION_MUTE_ESKALASI).

    Return True jika mute BERHASIL DIANTRIKAN (bukan berarti sudah dieksekusi
    — eksekusi & notifikasi terjadi async di moderation_worker_loop).
    False jika belum mencapai ambang atau masih dalam masa mute aktif.
    """
    cid    = message.chat.id
    uid    = message.from_user.id
    now_ts = time.time()

    mute_rec = await get_local_mute(cid, uid)

    # Jika user masih dalam masa mute → jangan tambah hitungan / mute lagi
    if mute_rec.get("muted_until", 0.0) > now_ts:
        return False

    updated = await increment_local_spam(cid, uid)
    consec  = updated.get("consec_spam", 1)

    if consec < SPAM_MUTE_THRESHOLD:
        return False

    # Ambang tercapai → terapkan mute eskalasi
    duration_secs, level_before = await apply_local_mute(cid, uid)
    duration_min                = duration_secs // 60

    async def _on_mute_done(success: bool):
        if not success:
            # FIXED: muted_until sudah ditulis oleh apply_local_mute() di atas
            # SEBELUM tahu hasil eksekusi API. Jika API gagal (bot bukan admin,
            # kehilangan izin restrict, dll), state mute palsu itu HARUS
            # dirollback — supaya pesan user berikutnya tidak terus-menerus
            # dihapus berdasarkan status mute yang sebenarnya tidak pernah
            # terjadi di Telegram.
            await revert_failed_local_mute(cid, uid, level_before)
            # Peringatkan admin/owner lewat LOG_CHANNEL — sebelumnya kegagalan
            # mute diam-diam saja tanpa sinyal apapun.
            asyncio.create_task(_log_mute_failed(client, message, spam_type))
            return

        # Beri tahu grup (pesan singkat, hapus 10 detik)
        spam_type_safe = html.escape(spam_type)
        notif = await send_group_notice(
            client, cid,
            f"{message.from_user.mention} di-mute {duration_min} menit "
            f"karena 10x pelanggaran berulang.",
            notice_kind="mute",
            parse_mode=ParseMode.HTML,
        )
        if notif is not None:
            asyncio.create_task(auto_delete_reply([notif], delay=10))

        # Log ke channel + per-grup action log (non-blocking)
        asyncio.create_task(_log_mute(
            client, message, duration_min, cid, uid, spam_type, konten
        ))

    await queue_mute(cid, uid, duration_secs, on_done=_on_mute_done)
    return True


async def _log_mute_failed(client, message, spam_type: str) -> None:
    """
    Peringatkan owner/admin via LOG_CHANNEL saat eksekusi mute API gagal
    (biasanya karena bot bukan admin grup atau kehilangan izin restrict).
    """
    from plugins.commands.log import _send_log, _fmt_waktu, _user_line

    uid          = message.from_user.id
    cid          = message.chat.id
    user_mention = _user_line(uid, message.from_user.first_name)

    log_text = (
        f"<b>❖ {format_violation_header(VIOLATION_MUTE_GAGAL)} ❖</b>\n"
        f"◈ <b>User:</b> {user_mention}\n"
        f"◈ <b>Grup:</b> {html.escape(message.chat.title)} (<code>{cid}</code>)\n"
        f"◈ <b>Waktu:</b> {_fmt_waktu()}\n"
        f"◈ <b>Alasan:</b> 10x pelanggaran berulang — tapi mute gagal dieksekusi\n"
        f"◈ <b>Penyebab:</b> Bot tidak punya izin admin untuk membatasi member\n"
        f"<i>⚠️ User tidak benar-benar di-mute — segera cek izin admin bot di grup ini.</i>"
    )
    await _send_log(client, log_text)


async def _log_mute(
    client,
    message,
    duration_min: int,
    cid: int,
    uid: int,
    spam_type: str,
    konten: str,
) -> None:
    """Log aksi mute eskalasi ke group action log dan LOG_CHANNEL."""
    from plugins.commands.log import _send_log, _fmt_waktu, _user_line

    user_name = message.from_user.first_name or str(uid)

    try:
        await insert_group_action_log(
            cid, "MUTE",
            f"Mute {duration_min} mnt — melanggar 10× berturut-turut",
            uid, user_name, konten,
            jenis=VIOLATION_MUTE_ESKALASI,
        )
    except Exception:
        pass

    if not LOG_CHANNEL:
        return

    user_mention = _user_line(uid, user_name)

    log_text = (
        f"<b>❖ {format_violation_header(VIOLATION_MUTE_ESKALASI)} ❖</b>\n"
        f"◈ <b>User:</b> {user_mention}\n"
        f"◈ <b>Grup:</b> {html.escape(message.chat.title)} (<code>{cid}</code>)\n"
        f"◈ <b>Waktu:</b> {_fmt_waktu()}\n"
        f"◈ <b>Durasi mute:</b> {duration_min} menit\n"
        f"◈ <b>Alasan:</b> 10x pelanggaran berulang (apapun jenisnya)\n\n"
        f"📨 <b>Pesan terakhir:</b>\n<code>{html.escape(konten[:300])}</code>"
    )
    await _send_log(client, log_text)
