"""
plugins/commands/stickerpack_owner.py
────────────────────────────────────────
Perintah owner via DM bot untuk mengelola daftar blokir stiker pack global
(lihat core/sticker_guard.py & plugins/commands/reportsticker.py):

  /cekstickerpack             — daftar semua pack yang masuk blokir global.
                                 Tiap entri dilengkapi tombol "👁 Lihat Stiker"
                                 untuk preview contoh stiker dari pack itu.
  /cekreport                  — daftar semua pack yang SUDAH dilaporkan
                                 tapi BELUM mencapai ambang blokir. 1 pesan,
                                 maks 10 entri per halaman, tombol Prev/Next
                                 untuk halaman berikutnya/sebelumnya. Tiap
                                 entri punya tautan langsung "LIHAT STICKERPACK"
                                 (t.me/addstickers/...), tanpa tombol inline.
  /openstikerpack SET_NAME    — buka blokir pack itu & reset hitungan
                                 laporan + daftar pelapor ke 0.
"""

import os
import time
import html

from pyrogram import Client, filters
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode

from core.sticker_guard import (
    get_blacklisted_packs, get_pending_packs, open_sticker_pack,
    sticker_report_db, REPORT_THRESHOLD,
)

_OWNER_ID = int(os.environ.get("OWNER_ID", 0))


def _fmt_ts(ts) -> str:
    if not ts:
        return "-"
    try:
        return time.strftime("%d-%m-%Y %H:%M", time.localtime(ts))
    except Exception:
        return "-"


@Client.on_message(filters.command("cekstickerpack") & filters.private & filters.user(_OWNER_ID))
async def cmd_cek_stickerpack(client: Client, message: Message):
    if not _OWNER_ID:
        return

    packs = await get_blacklisted_packs()
    if not packs:
        return await message.reply(
            "✅ <b>Tidak ada stiker pack yang diblokir saat ini.</b>",
            parse_mode=ParseMode.HTML,
        )

    packs.sort(key=lambda d: d.get("blacklisted_at") or 0, reverse=True)

    await message.reply(
        f"🚫 <b>Daftar Stiker Pack Diblokir</b> ({len(packs)})\n\n"
        f"<i>Tap \"Lihat Stiker\" untuk preview contoh stiker dari pack.</i>",
        parse_mode=ParseMode.HTML,
    )

    for doc in packs:
        set_name = doc.get("_id") or doc.get("set_name")
        title    = html.escape(doc.get("set_title") or set_name)
        count    = doc.get("count", 0)
        when     = _fmt_ts(doc.get("blacklisted_at"))

        text = (
            f"📦 <b>{title}</b>\n"
            f"◈ <b>Set name:</b> <code>{html.escape(set_name)}</code>\n"
            f"◈ <b>Jumlah laporan:</b> {count}\n"
            f"◈ <b>Diblokir sejak:</b> {when}"
        )
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("👁 Lihat Stiker", callback_data=f"stkview_{set_name}"),
        ]])
        try:
            await message.reply(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)
        except Exception as e:
            print(f"[StickerOwner] gagal kirim entri pack {set_name}: {e}")


_PAGE_SIZE = 10


def _build_report_page_text(packs: list[dict], page: int) -> tuple[str, int]:
    """Susun teks 1 halaman (maks 10 entri) dari daftar pack pending.
    Return (text, total_pages)."""
    total_pages = max(1, (len(packs) + _PAGE_SIZE - 1) // _PAGE_SIZE)
    page = max(1, min(page, total_pages))
    start = (page - 1) * _PAGE_SIZE
    chunk = packs[start:start + _PAGE_SIZE]

    entries = []
    for doc in chunk:
        set_name = doc.get("_id") or doc.get("set_name")
        title    = html.escape(doc.get("set_title") or set_name)
        count    = doc.get("count", 0)
        sisa     = max(0, REPORT_THRESHOLD - count)
        when     = _fmt_ts(doc.get("updated_at"))
        link     = f"https://t.me/addstickers/{set_name}"

        entries.append(
            f"📦 <b>{title}</b>\n"
            f"◈ Set name: <code>{html.escape(set_name)}</code>\n"
            f"◈ Jumlah laporan: {count}/{REPORT_THRESHOLD} (butuh {sisa} lagi)\n"
            f"◈ Laporan terakhir: {when}\n"
            f'◈ <a href="{link}">LIHAT STICKERPACK</a>'
        )

    header = (
        f"🟡 <b>Pack Dalam Progres Laporan</b> ({len(packs)})\n"
        f"<i>Halaman {page}/{total_pages} — belum mencapai ambang {REPORT_THRESHOLD} laporan.</i>\n\n"
    )
    body = "\n\n".join(entries) if entries else "<i>Tidak ada entri di halaman ini.</i>"
    return header + body, total_pages


def _build_report_page_keyboard(page: int, total_pages: int) -> InlineKeyboardMarkup | None:
    if total_pages <= 1:
        return None
    row = []
    if page > 1:
        row.append(InlineKeyboardButton("◀️ Prev", callback_data=f"ckrpt_{page - 1}"))
    if page < total_pages:
        row.append(InlineKeyboardButton("▶️ Next", callback_data=f"ckrpt_{page + 1}"))
    return InlineKeyboardMarkup([row]) if row else None


@Client.on_message(filters.command("cekreport") & filters.private & filters.user(_OWNER_ID))
async def cmd_cek_report(client: Client, message: Message):
    if not _OWNER_ID:
        return

    packs = await get_pending_packs()
    if not packs:
        return await message.reply(
            "✅ <b>Tidak ada pack yang sedang dalam progres laporan.</b>",
            parse_mode=ParseMode.HTML,
        )

    packs.sort(key=lambda d: d.get("count", 0), reverse=True)
    text, total_pages = _build_report_page_text(packs, 1)
    keyboard = _build_report_page_keyboard(1, total_pages)

    await message.reply(
        text, reply_markup=keyboard,
        parse_mode=ParseMode.HTML, disable_web_page_preview=True,
    )


@Client.on_callback_query(filters.regex(r"^ckrpt_(\d+)$") & filters.user(_OWNER_ID))
async def cb_cek_report_page(client: Client, cb: CallbackQuery):
    page = int(cb.matches[0].group(1))

    packs = await get_pending_packs()
    if not packs:
        await cb.answer("✅ Tidak ada pack yang sedang dalam progres laporan.", show_alert=True)
        try:
            await cb.message.delete()
        except Exception:
            pass
        return

    packs.sort(key=lambda d: d.get("count", 0), reverse=True)
    text, total_pages = _build_report_page_text(packs, page)
    keyboard = _build_report_page_keyboard(page, total_pages)

    await cb.answer()
    try:
        await cb.message.edit_text(
            text, reply_markup=keyboard,
            parse_mode=ParseMode.HTML, disable_web_page_preview=True,
        )
    except Exception as e:
        print(f"[StickerOwner] gagal edit halaman /cekreport: {e}")


@Client.on_callback_query(filters.regex(r"^stkview_(.+)$") & filters.user(_OWNER_ID))
async def cb_view_sticker(client: Client, cb: CallbackQuery):
    set_name = cb.matches[0].group(1)
    doc = await sticker_report_db.find_one({"_id": set_name})

    if not doc or not doc.get("sample_file_id"):
        return await cb.answer("❌ Contoh stiker tidak tersedia.", show_alert=True)

    await cb.answer()
    try:
        await client.send_sticker(cb.from_user.id, doc["sample_file_id"])
    except Exception as e:
        await client.send_message(
            cb.from_user.id,
            f"❌ Gagal kirim preview stiker:\n<code>{html.escape(str(e))}</code>",
            parse_mode=ParseMode.HTML,
        )


@Client.on_message(filters.command("openstikerpack") & filters.private & filters.user(_OWNER_ID))
async def cmd_open_stickerpack(client: Client, message: Message):
    if not _OWNER_ID:
        return

    if len(message.command) < 2:
        return await message.reply(
            "❌ <b>Format Salah</b>\n\n"
            "Gunakan: <code>/openstikerpack SET_NAME</code>\n\n"
            "Lihat SET_NAME lewat /cekstickerpack (tertulis di tiap entri).",
            parse_mode=ParseMode.HTML,
        )

    set_name = message.command[1].strip()
    ok = await open_sticker_pack(set_name)

    if not ok:
        return await message.reply(
            f"❌ Pack <code>{html.escape(set_name)}</code> tidak ditemukan di database.",
            parse_mode=ParseMode.HTML,
        )

    await message.reply(
        f"✅ <b>Pack dibuka dari blokir.</b>\n\n"
        f"<code>{html.escape(set_name)}</code> dihapus dari daftar blokir global. "
        f"Hitungan laporan &amp; daftar pelapor direset ke 0 — pack ini bisa "
        f"dilaporkan ulang dari awal.",
        parse_mode=ParseMode.HTML,
    )
