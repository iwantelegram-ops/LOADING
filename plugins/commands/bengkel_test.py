"""
plugins/commands/bengkel_test.py
─────────────────────────────────────────────────────────────────────────────
Perintah debug khusus owner untuk menguji WorkshopJoinPool secara manual.

  /bjoin  — paksa 1 bot bengkel idle masuk ke grup ini (lewat prosedur
            join+verifikasi & mode kerja gabungan yang SAMA seperti yang
            dipakai monitor_loop otomatis — bukan add_chat_members mentah,
            supaya /bjoin betul-betul mencerminkan perilaku produksi,
            termasuk constraint "tidak boleh kerja kalau belum confirmed
            joined").
  /bleave — keluarkan SEMUA bot bengkel yang sedang membantu grup ini
            (hentikan rotasi, unfreeze instance pemantau, leave_chat
            satu-persatu).
  /bstatus — status semua bot bengkel di pool.

Hanya bisa dipakai di GRUP (bukan DM), oleh OWNER saja.
"""

import os
import time
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.enums import ParseMode

_OWNER_ID = int(os.environ.get("OWNER_ID", 0))

_GROUP_FILTER = (filters.group | filters.forum) & filters.user(_OWNER_ID)


# ── /bjoin ────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("bjoin") & _GROUP_FILTER)
async def cmd_bjoin(client: Client, message: Message):
    """
    /bjoin — paksa 1 bengkel idle masuk ke grup ini lewat prosedur
    join+verifikasi resmi (_assign_bengkel). Kalau grup ini BELUM dalam mode
    kerja gabungan, instance pemantau akan ikut di-freeze & masuk rotasi
    juga (sama seperti yang terjadi otomatis saat FloodWait sungguhan).
    """
    if not _OWNER_ID:
        return

    from core.workshop_join_pool import workshop_join_pool
    from monitor_bot_reference import _active_instances

    chat_id = message.chat.id

    instance = _active_instances.get(chat_id)
    if instance is None:
        return await message.reply(
            "⚠️ Tidak ada MonitorInstance aktif untuk grup ini — "
            "bot pemantau belum/tidak berjalan di sini.",
            parse_mode=ParseMode.HTML,
        )

    existing = workshop_join_pool.bengkels_for(chat_id)
    if existing:
        names = ", ".join(f"#{b.index}" for b in existing)
        return await message.reply(
            f"ℹ️ Sudah ada Bengkel ({names}) membantu grup ini.\n"
            f"Gunakan /bleave dulu untuk mengeluarkan semuanya, "
            f"atau biarkan saja kalau memang sedang ingin tes grup ramai "
            f"(akan menambah peserta rotasi baru).",
            parse_mode=ParseMode.HTML,
        )

    bengkel = workshop_join_pool._pick_idle_bengkel()
    if bengkel is None:
        total = workshop_join_pool.size
        busy = [b.index for b in workshop_join_pool._bengkels if not b.is_idle]
        return await message.reply(
            f"⚠️ Tidak ada bengkel idle saat ini.\nTotal: {total} | Sibuk: {busy}",
            parse_mode=ParseMode.HTML,
        )

    msg = await message.reply(
        f"⏳ Mencoba join+verifikasi Bengkel <b>#{bengkel.index}</b> "
        f"(user_id=<code>{bengkel.user_id}</code>, @{bengkel.username}) "
        f"ke grup ini...",
        parse_mode=ParseMode.HTML,
    )

    # _assign_bengkel: add_chat_members (lewat userbot, hanya kalau userbot
    # admin dengan hak invite) → get_chat_member retry untuk KONFIRMASI
    # status member → baru kalau confirmed, masuk mode kerja gabungan.
    await workshop_join_pool._assign_bengkel(bengkel, chat_id, instance)

    if bengkel.joined and bengkel.assigned_chat_id == chat_id:
        rotation = workshop_join_pool._rotations.get(chat_id)
        n = rotation.participant_count if rotation else 1
        await msg.edit_text(
            f"✅ Bengkel <b>#{bengkel.index}</b> (@{bengkel.username}) "
            f"CONFIRMED JOIN grup ini.\n"
            f"Mode kerja gabungan aktif ({n} peserta, round-robin).\n"
            f"Gunakan /bleave untuk mengeluarkan.",
            parse_mode=ParseMode.HTML,
        )
    else:
        await msg.edit_text(
            f"❌ Bengkel <b>#{bengkel.index}</b> GAGAL dikonfirmasi join grup ini.\n"
            f"Kemungkinan: userbot bukan admin / tidak punya hak invite di grup ini, "
            f"atau add_chat_members ditolak Telegram. Cek log untuk detail.\n"
            f"Bengkel sudah dikembalikan ke status idle (tidak ada kerja yang "
            f"dipaksakan tanpa konfirmasi join).",
            parse_mode=ParseMode.HTML,
        )


# ── /bleave ───────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("bleave") & _GROUP_FILTER)
async def cmd_bleave(client: Client, message: Message):
    """
    /bleave — keluarkan SEMUA bengkel yang sedang membantu grup ini, hentikan
    rotasi, unfreeze instance pemantau (lewat _release_group — sama seperti
    yang dipanggil otomatis saat grup dianggap "aman").
    """
    if not _OWNER_ID:
        return

    from core.workshop_join_pool import workshop_join_pool

    chat_id = message.chat.id

    existing = workshop_join_pool.bengkels_for(chat_id)
    if not existing:
        lines = []
        for b in workshop_join_pool._bengkels:
            status = f"grup {b.assigned_chat_id}" if not b.is_idle else "idle"
            lines.append(f"  Bengkel #{b.index} (@{b.username}) — {status}")
        return await message.reply(
            "ℹ️ Tidak ada bengkel yang assigned di grup ini.\n\n"
            "Status semua bengkel:\n" + "\n".join(lines),
            parse_mode=ParseMode.HTML,
        )

    names = ", ".join(f"#{b.index}" for b in existing)
    msg = await message.reply(
        f"⏳ Mengeluarkan Bengkel ({names}) dari grup ini &amp; menghentikan "
        f"mode kerja gabungan...",
        parse_mode=ParseMode.HTML,
    )

    await workshop_join_pool._release_group(chat_id, reason="manual /bleave")

    await msg.edit_text(
        f"✅ Bengkel ({names}) berhasil keluar. Instance pemantau kembali "
        f"bekerja sendiri seperti biasa.",
        parse_mode=ParseMode.HTML,
    )


# ── /bstatus ──────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("bstatus") & _GROUP_FILTER)
async def cmd_bstatus(client: Client, message: Message):
    """
    /bstatus — tampilkan status semua bot bengkel di pool + ringkasan
    rotasi kerja gabungan yang sedang aktif.
    """
    if not _OWNER_ID:
        return

    from core.workshop_join_pool import workshop_join_pool

    lines = [f"<b>🔧 Status Bengkel Pool ({workshop_join_pool.size} bot)</b>\n"]
    now = time.monotonic()

    for b in workshop_join_pool._bengkels:
        if not b._started:
            status = "❌ belum start"
        elif b.is_floodwait(now):
            sisa = int(b.busy_until - now)
            status = f"⏳ FloodWait ({sisa}s lagi)"
        elif b.is_idle:
            status = "✅ idle"
        elif not b.joined:
            status = f"⏳ join belum confirmed → grup <code>{b.assigned_chat_id}</code>"
        else:
            status = f"🔄 kerja gabungan → grup <code>{b.assigned_chat_id}</code>"

        lines.append(
            f"  <b>#{b.index}</b> @{b.username} "
            f"(id=<code>{b.user_id}</code>) — {status}"
        )

    if workshop_join_pool._rotations:
        lines.append("\n<b>Rotasi kerja gabungan aktif:</b>")
        for chat_id, rotation in workshop_join_pool._rotations.items():
            lines.append(
                f"  grup <code>{chat_id}</code> — {rotation.participant_count} peserta "
                f"(pemantau + {len(rotation.bengkels)} bengkel)"
            )

    pending = workshop_join_pool._pending_groups
    if pending:
        lines.append(f"\n⏳ Antrian pending: {pending}")

    await message.reply("\n".join(lines), parse_mode=ParseMode.HTML)
