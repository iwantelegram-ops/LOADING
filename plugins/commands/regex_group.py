import asyncio
import re
import html
import unicodedata

"""
plugins/commands/regex_group.py
────────────────────────────────
Perintah admin grup untuk kelola regex lokal:
  /addgroupregex, /delgroupregex, /listgroupregex

FORMAT INPUT:
  kata | kata | kata
  Pisahkan kata dengan | (tanda pipa). Setiap kata diproses dengan AI mutasi
  dan dirakit menjadi pola interlock AND. Semua kata HARUS hadir sekaligus
  dalam satu pesan agar filter aktif.

  Contoh:
    /addgroupregex togel
    /addgroupregex jual | akun
    /addgroupregex promo | slot | link

  Semantik | adalah AND (bukan OR) — sama seperti sistem owner spam Nexus.
"""

from pyrogram import Client, filters
from pyrogram.enums import ParseMode

from database import db, is_admin, auto_delete_reply
from core.regex_utils import build_group_interlock

# Alias untuk kompatibilitas file lain yang masih import nama lama
_build_group_interlock = build_group_interlock

group_regex_db = db["regex_per_group"]
DELAY_NOTIF    = 10


@Client.on_message(filters.command("addgroupregex") & (filters.group | filters.forum))
async def add_group_regex(client: Client, message):
    cid = message.chat.id
    uid = message.from_user.id if message.from_user else None
    if not await is_admin(client, cid, uid):
        return

    if len(message.command) < 2:
        res = await message.reply(
            "<b>❖ FORMAT INPUT ❖</b>\n\n"
            "⚠️ <b>Cara penggunaan:</b>\n"
            "<code>/addgroupregex [kata]</code>\n\n"
            "<b>✦ Contoh — 1 kata:</b>\n"
            "<code>/addgroupregex togel</code>\n\n"
            "<b>✦ Contoh — 2 kata (AND, harus ada keduanya):</b>\n"
            "<code>/addgroupregex jual | akun</code>\n\n"
            "<b>✦ Contoh — 3 kata (AND):</b>\n"
            "<code>/addgroupregex promo | slot | link</code>\n\n"
            "⚠️ Tanda <code>|</code> = AND (semua kata harus ada sekaligus).\n"
            "Setiap kata diproses AI mutasi otomatis.",
            parse_mode=ParseMode.HTML
        )
        asyncio.create_task(auto_delete_reply([res, message], delay=DELAY_NOTIF))
        return

    # Identik dengan owner: ambil teks setelah command, normalize NFKC dulu
    raw_input = unicodedata.normalize("NFKC", message.text.split(None, 1)[1].strip())

    try:
        pola, mutasi_display, _ = build_group_interlock(raw_input)
        re.compile(pola)
    except (ValueError, re.error) as e:
        res = await message.reply(
            f"<b>❖ ERROR ❖</b>\n\n"
            f"❌ <b>Input Gagal Diproses!</b>\n"
            f"◈ <b>Input:</b> <code>{html.escape(raw_input)}</code>\n"
            f"◈ <b>Keterangan:</b> <code>{html.escape(str(e))}</code>",
            parse_mode=ParseMode.HTML
        )
        asyncio.create_task(auto_delete_reply([res, message], delay=DELAY_NOTIF))
        return

    # mutasi_display dari build_group_interlock sudah identik dengan owner:
    # list[tuple[str, list[str]]] → (kata_lowercase, mutasi_list)
    # Tidak perlu re-generate manual — mutasi di sini SAMA dengan yang di pola
    kata_list   = [k for k, _ in mutasi_display]
    mutasi_map  = {k: m for k, m in mutasi_display}
    raw_display = " | ".join(kata_list) if kata_list else raw_input

    await group_regex_db.update_one(
        {"chat_id": cid, "pattern": pola},
        {"$set": {
            "chat_id":   cid,
            "pattern":   pola,
            "pola":      pola,
            "raw":       raw_display,
            "kata_list": kata_list,
            "mutasi":    mutasi_map,
        }},
        upsert=True,
    )

    try:
        from plugins.filters.antispam import invalidate_local_regex_cache
        invalidate_local_regex_cache(cid)
    except Exception:
        pass

    kata_str = " + ".join(f"<code>{html.escape(k)}</code>" for k in kata_list)
    res = await message.reply(
        f"<b>❖ FILTER KATA DITAMBAHKAN ❖</b>\n\n"
        f"✅ <b>Filter Khusus Grup Berhasil Tersimpan!</b>\n"
        f"◈ <b>Kata Kunci:</b> {kata_str}\n"
        f"◈ <b>Semantik:</b> Semua kata wajib ada sekaligus (AND)\n"
        f"◈ <b>Mutasi:</b> Otomatis mendeteksi variasi huruf &amp; leet\n\n"
        f"<i>Gunakan /listgroupregex untuk melihat semua filter aktif.</i>",
        parse_mode=ParseMode.HTML
    )
    asyncio.create_task(auto_delete_reply([res, message], delay=DELAY_NOTIF))


@Client.on_message(filters.command("delgroupregex") & (filters.group | filters.forum))
async def del_group_regex(client: Client, message):
    cid = message.chat.id
    uid = message.from_user.id if message.from_user else None
    if not await is_admin(client, cid, uid):
        return

    if len(message.command) < 2:
        res = await message.reply(
            "⚠️ <b>Format Input Salah</b>\n"
            "<b>Format:</b> <code>/delgroupregex [kata]</code>\n\n"
            "Gunakan kata yang sama seperti saat menambahkan.",
            parse_mode=ParseMode.HTML
        )
        asyncio.create_task(auto_delete_reply([res, message], delay=DELAY_NOTIF))
        return

    # Identik dengan owner: normalize NFKC dulu
    raw_input      = unicodedata.normalize("NFKC", message.text.split(None, 1)[1].strip())
    mutasi_display = []
    result         = None

    try:
        pola, mutasi_display, _ = build_group_interlock(raw_input)
        result = await group_regex_db.delete_one({"chat_id": cid, "pattern": pola})
    except (ValueError, re.error):
        pass

    if not result or not result.deleted_count:
        # Fallback: cari by raw display (kata_list joined)
        kata_list   = [k for k, _ in mutasi_display] if mutasi_display else []
        raw_display = " | ".join(kata_list) if kata_list else raw_input.strip()
        result = await group_regex_db.delete_one({"chat_id": cid, "raw": raw_display})

    if result and result.deleted_count:
        try:
            from plugins.filters.antispam import invalidate_local_regex_cache
            invalidate_local_regex_cache(cid)
        except Exception:
            pass
        res = await message.reply(
            f"🗑️ <b>Filter Grup Berhasil Dihapus!</b>\n"
            f"◈ <b>Kata:</b> <code>{html.escape(raw_input)}</code>",
            parse_mode=ParseMode.HTML
        )
    else:
        res = await message.reply(
            "❌ <b>Kata Tidak Ditemukan di Daftar Filter Grup Ini.</b>\n"
            "Gunakan /listgroupregex untuk melihat daftar yang aktif.",
            parse_mode=ParseMode.HTML
        )
    asyncio.create_task(auto_delete_reply([res, message], delay=DELAY_NOTIF))


@Client.on_message(filters.command("listgroupregex") & (filters.group | filters.forum))
async def list_group_regex(client: Client, message):
    cid = message.chat.id
    uid = message.from_user.id if message.from_user else None
    if not await is_admin(client, cid, uid):
        return

    docs = [doc async for doc in group_regex_db.find({"chat_id": cid})]

    if docs:
        lines = "\n".join(f"  ◈ <b>{html.escape(str(doc.get('raw', '—')))}</b>" for doc in docs)
        text  = (
            "<b>❖ FILTER KATA GRUP ❖</b>\n"
            f"⚡ <b>Total Aktif:</b> <code>{len(docs)} Pola</code>\n\n"
            "<b>▰▰▰ DAFTAR KATA DIBLOKIR ▰▰▰</b>\n"
            f"{lines}\n\n"
            "<i>(Aturan di atas hanya berjalan eksklusif di grup ini)\n"
            "Semua entri menggunakan deteksi mutasi otomatis.</i>"
        )
    else:
        text = (
            "<b>❖ FILTER KATA GRUP ❖</b>\n\n"
            "📭 <b>Daftar filter kata di grup ini masih kosong.</b>\n"
            "Gunakan <code>/addgroupregex kata</code> untuk menambah aturan baru."
        )

    res = await message.reply(text, parse_mode=ParseMode.HTML)
    asyncio.create_task(auto_delete_reply([res, message], delay=30))
