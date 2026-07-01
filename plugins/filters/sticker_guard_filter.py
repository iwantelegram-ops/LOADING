"""
plugins/filters/sticker_guard_filter.py
─────────────────────────────────────────
Penegakan blokir stiker pack global di latar belakang — bagian dari sistem
/reportsticker (lihat plugins/commands/reportsticker.py & core/sticker_guard.py).

Setiap stiker yang masuk ke grup manapun dicek terhadap cache daftar blokir
global (TTL — lihat core/sticker_guard.py, REFRESH otomatis tiap 60 detik
atau seketika saat ada pack baru masuk blokir). Jika set_name-nya cocok:
  • Pesan dihapus.
  • Pengirim kena hukuman eskalasi lewat core/punishment.py — sistem
    terpusat yang sama dipakai semua filter spam lain di proyek ini.
  • Dicatat ke group_action_log per grup + LOG_CHANNEL.

Tidak ada toggle per grup untuk fitur ini — selalu aktif SELAMA bot punya
izin hapus+restrict di grup tersebut (check_bot_permissions — sama seperti
filter lain: kalau bot bukan admin, fitur ini otomatis diam, bukan error).
"""

import os
import html
import asyncio

from pyrogram import Client, filters

from database import (
    is_admin, check_bot_permissions,
    mark_message_handled, is_message_handled,
    insert_group_action_log,
)
from core.sticker_guard import is_blacklisted
from core.punishment import check_and_punish
from core.violation_types import VIOLATION_STICKER_BLACKLIST, format_violation_header


@Client.on_message((filters.group | filters.forum) & filters.sticker, group=3)
async def sticker_guard_filter(client: Client, message):
    if not message.from_user:
        return

    cid, uid, mid = message.chat.id, message.from_user.id, message.id
    if is_message_handled(cid, mid):
        return

    set_name = message.sticker.set_name if message.sticker else None
    if not set_name:
        return

    if not await is_blacklisted(set_name):
        return

    if await is_admin(client, cid, uid):
        return  # admin tidak ditindak otomatis — selaras filter lain

    if not await check_bot_permissions(client, cid):
        return  # bot tak punya izin → diam, perm_watchdog yang urus status grup

    mark_message_handled(cid, mid)

    try:
        await message.delete()
    except Exception:
        pass

    asyncio.create_task(
        check_and_punish(
            client, message, "STICKER_BLACKLIST_GLOBAL",
            f"stiker pack: {set_name}",
        )
    )
    asyncio.create_task(_log_auto_delete(client, message, set_name))


async def _log_auto_delete(client, message, set_name: str) -> None:
    from plugins.commands.log import _send_log, _fmt_waktu, _user_line

    uid          = message.from_user.id
    cid          = message.chat.id
    user_mention = _user_line(uid, message.from_user.first_name)

    try:
        await insert_group_action_log(
            cid, "HAPUS",
            f"Stiker dari pack terblokir: {set_name}",
            uid, message.from_user.first_name or str(uid), set_name,
            jenis=VIOLATION_STICKER_BLACKLIST,
        )
    except Exception:
        pass

    if not int(os.environ.get("LOG_CHANNEL", 0)):
        return

    log_text = (
        f"<b>❖ {format_violation_header(VIOLATION_STICKER_BLACKLIST)} ❖</b>\n"
        f"◈ <b>User:</b> {user_mention}\n"
        f"◈ <b>Grup:</b> {html.escape(message.chat.title)} (<code>{cid}</code>)\n"
        f"◈ <b>Waktu:</b> {_fmt_waktu()}\n"
        f"◈ <b>Pack:</b> <code>{html.escape(set_name)}</code>\n"
        f"<i>Stiker otomatis dihapus — pack ini sudah ada di daftar blokir global.</i>"
    )
    await _send_log(client, log_text)
