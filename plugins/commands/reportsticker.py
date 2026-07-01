"""
plugins/commands/reportsticker.py
──────────────────────────────────
Perintah member /reportsticker — laporkan stiker pack yang dianggap spam /
tidak pantas dengan me-reply pesan stiker di grup.

ATURAN:
  • 1 user hanya bisa melaporkan 1 pack yang sama SEKALI — laporan ulang
    untuk pack yang sama tidak menambah hitungan apapun (tidak terbatas
    waktu, sampai pack dibuka lagi oleh owner via /openstikerpack).
  • Pack yang berbeda boleh dilaporkan berkali-kali tanpa cooldown, di
    grup manapun — tidak ada batas lintas pack.
  • Aturan di atas diabaikan SECARA SENYAP (tanpa notif/balasan) —
    sesuai permintaan, supaya tidak menambah noise di grup.
  • API GetStickerSet hanya dipanggil saat pack itu BENAR-BENAR baru
    (belum pernah tercatat sama sekali) — lihat core/sticker_guard.py.
  • Begitu hitungan pelapor unik mencapai REPORT_THRESHOLD (5) → pack
    masuk daftar blokir global, pesan yang di-reply langsung dihapus, dan
    pengirimnya kena hukuman eskalasi lewat core/punishment.py (sistem
    yang sama dipakai semua filter lain).
  • Tidak ada toggle per grup — perintah ini selalu aktif di semua grup.
  • TIDAK memanggil API Telegram apapun (GetStickerSet dihapus) — judul
    yang ditampilkan di notif/log adalah `set_name` (short_name) apa
    adanya, supaya 100% hemat API & tidak kena risiko FloodWait sama
    sekali walau diklik serentak oleh banyak user di banyak grup.
  • Laporan yang BERHASIL tercatat (pack baru/beda, belum capai ambang)
    tetap dapat 1 balasan singkat auto-delete (3 detik) sebagai bukti
    command-nya jalan — TIDAK 100% bisu lagi seperti sebelumnya, supaya
    gampang dibedakan dari kasus "diabaikan" (cooldown/pack sama).
"""

import os
import html
import asyncio
import traceback

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode

from database import check_bot_permissions, auto_delete_reply
from core.sticker_guard import has_reported, record_report, REPORT_THRESHOLD
from core.punishment import check_and_punish
from core.group_notify import send_group_notice


@Client.on_message(filters.command("reportsticker") & filters.group)
async def cmd_reportsticker(client: Client, message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id

    replied = message.reply_to_message
    if not replied or not replied.sticker:
        return await message.reply(
            "↩️ Reply stiker yang ingin dilaporkan dengan <code>/reportsticker</code>.",
            parse_mode=ParseMode.HTML,
        )

    set_name = replied.sticker.set_name
    if not set_name:
        return  # stiker lepas tanpa pack — tidak bisa dilaporkan, diam saja

    try:
        # ── User ini sudah pernah melaporkan pack ini — senyap, tanpa notif ──
        if await has_reported(set_name, uid):
            return

        # ── Catat laporan — atomik, tanpa API call apapun (set_name = title) ─
        count, became_blacklisted = await record_report(
            set_name, set_name, uid, replied.sticker.file_id
        )
    except Exception as e:
        # Tidak boleh ketelan diam-diam — print traceback penuh ke console/log
        # supaya kalau DB error atau apapun, kelihatan jelas bukan disangka
        # "command-nya gagal tanpa jejak".
        print(f"[ReportSticker] ❌ ERROR saat catat laporan pack={set_name} uid={uid}: {e}")
        traceback.print_exc()
        return

    set_title = set_name

    if not became_blacklisted:
        # ── Laporan baru tercatat tapi belum capai ambang — bukti command
        # jalan, auto-delete cepat biar tidak menambah noise permanen ────────
        try:
            confirm = await message.reply(
                f"✅ Laporan tercatat ({count}/{REPORT_THRESHOLD}).",
                parse_mode=ParseMode.HTML,
            )
            asyncio.create_task(auto_delete_reply([confirm, message], delay=3))
        except Exception:
            pass
        return

    # ── Ambang tercapai → eksekusi blokir langsung di pesan ini ──────────────
    cid = message.chat.id
    if not await check_bot_permissions(client, cid):
        return  # bot tak punya izin hapus/restrict — biarkan watchdog yang urus

    offender = replied.from_user

    try:
        await replied.delete()
    except Exception:
        pass

    if offender:
        asyncio.create_task(
            check_and_punish(
                client, replied, "STICKER_BLACKLIST_GLOBAL",
                f"stiker pack: {set_title}",
            )
        )

    asyncio.create_task(_log_blacklist_trigger(client, replied, set_name, set_title))

    notif = await send_group_notice(
        client, cid,
        f"🚫 Stiker pack <b>{html.escape(set_title)}</b> mencapai {REPORT_THRESHOLD} laporan "
        f"dan diblokir secara global. Pesan dihapus.",
        notice_kind="sticker_blacklist",
        parse_mode=ParseMode.HTML,
    )
    if notif is not None:
        asyncio.create_task(auto_delete_reply([notif], delay=10))


async def _log_blacklist_trigger(client, replied, set_name: str, set_title: str) -> None:
    """Catat momen pack PERTAMA KALI masuk blokir (dipicu oleh /reportsticker)."""
    from database import insert_group_action_log
    from core.violation_types import VIOLATION_STICKER_BLACKLIST, format_violation_header
    from plugins.commands.log import _send_log, _fmt_waktu, _user_line

    if not replied.from_user:
        return

    uid          = replied.from_user.id
    cid          = replied.chat.id
    user_mention = _user_line(uid, replied.from_user.first_name)

    try:
        await insert_group_action_log(
            cid, "HAPUS",
            f"Stiker pack mencapai ambang laporan & diblokir global: {set_title}",
            uid, replied.from_user.first_name or str(uid), set_name,
            jenis=VIOLATION_STICKER_BLACKLIST,
        )
    except Exception:
        pass

    if not int(os.environ.get("LOG_CHANNEL", 0)):
        return

    log_text = (
        f"<b>❖ {format_violation_header(VIOLATION_STICKER_BLACKLIST)} ❖</b>\n"
        f"◈ <b>User:</b> {user_mention}\n"
        f"◈ <b>Grup:</b> {html.escape(replied.chat.title)} (<code>{cid}</code>)\n"
        f"◈ <b>Waktu:</b> {_fmt_waktu()}\n"
        f"◈ <b>Pack:</b> {html.escape(set_title)} (<code>{html.escape(set_name)}</code>)\n"
        f"<i>Pack ini baru saja mencapai ambang laporan dan masuk daftar blokir global "
        f"— sejak ini akan otomatis dihapus di grup manapun ditemui.</i>"
    )
    await _send_log(client, log_text)
