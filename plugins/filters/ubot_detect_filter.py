"""
plugins/filters/ubot_detect_filter.py
─────────────────────────────────────────────────────────────────────────────
Filter bot biasa untuk fitur DETEKSI UBOT (pengganti "Agresif Spam" lama).

POSISI: group=0 — SAMA seperti agresif_spam_filter.py sebelumnya, dieksekusi
PERTAMA sebelum semua filter lain.
  CAS=-1 → (ubot_detect=0) → bio=1 → antispam=2 → nexus=5 → dll

Cara kerja (lihat core/ubot_detect.py untuk detail aturan lengkap):
  1. REKAM SELALU JALAN — record_sentence() dipanggil untuk SETIAP pesan
     teks dari user non-admin/non-VIP di grup ini, SEPENUHNYA INDEPENDEN
     dari toggle fitur lain (local/global/dll). Fitur ini TIDAK pernah
     bergantung pada status ON/OFF fitur lain — hanya bergantung pada
     toggle dirinya sendiri (ubot_detect) untuk EVALUASI & EKSEKUSI.
  2. EVALUASI & EKSEKUSI hanya jalan kalau ubot_detect=True untuk grup ini.
     Kalau semua kalimat user ini sudah ≥3× tanpa variasi baru DAN pesan
     ini sendiri cocok salah satu kalimat ≥3× itu → hapus + StopPropagation.
     Selain itu → lolos, filter lain tetap jalan normal.
  3. Setiap delete yang BERHASIL ikut diantrikan ke check_and_punish()
     (core/punishment.py) — sama seperti jenis spam lain (filter kata,
     link, mention, Nexus AI, dll). Artinya pelanggaran ubot juga menambah
     hitungan 10× berturut-turut menuju mute eskalasi, bukan cuma hapus
     pesan tanpa konsekuensi lanjutan.

MEMORI TERPISAH: Fitur ini memakai collection Mongo sendiri
  (ubot_sentence_tracker, lihat core/ubot_detect.py) — TERPISAH TOTAL dari
  collection seen_messages yang dipakai "Anti Duplikasi Lokal" di
  plugins/filters/antispam.py & core/antispam_queue.py. Dua fitur ini
  tidak boleh saling membaca/menulis collection atau cache milik satu
  sama lain dengan cara apapun.

PENTING: StopPropagation hanya dilempar SETELAH delete, TIDAK di finally —
  pola yang sama dengan filter lain di proyek ini (lihat agresif_spam_filter
  lama) supaya pesan yang lolos tidak ikut ter-StopPropagation.
"""

import asyncio
import html as _html
from pyrogram import Client, filters, StopPropagation
from pyrogram.errors import MessageDeleteForbidden, FloodWait, ChatAdminRequired

from database import is_admin, db, get_config, insert_group_action_log, check_bot_permissions
from core.punishment import check_and_punish
from core.violation_types import VIOLATION_NEXUS_AI, format_violation_header

free_col = db["free_per_group"]

# Kode violation khusus ubot detect — pakai NEXUS_AI karena secara
# konsep ini adalah deteksi pola perilaku otomatis, bukan filter kata manual.
_VIOLATION_UBOT = VIOLATION_NEXUS_AI


@Client.on_message(
    # FIX: filter ini sebelumnya cocok ke SEMUA teks termasuk command
    # ("/spam", "/delnexus", "/delkalimat", "/addregex", "/wlregex" di
    # plugins/nexus/*.py — sama-sama group=0). Karena folder plugins/filters
    # ke-load lebih dulu dari plugins/nexus (urutan alfabetis), handler ini
    # selalu menang duluan untuk command2 tsb dan Pyrogram cuma jalanin SATU
    # handler pertama yang match per group — command nexus jadi diam total
    # tanpa log. Tambah ~filters.regex(r"^/") supaya command apa pun (bukan
    # cuma yang disebut di atas) otomatis lewat ke handler aslinya.
    (filters.group | filters.forum) & filters.text & ~filters.service & ~filters.regex(r"^/"),
    group=0
)
async def ubot_detect_filter(client: Client, message):
    # Hanya user asli — bukan bot, bukan anonymous channel post
    if not message.from_user:
        return
    if message.from_user.is_bot:
        return

    raw_text = message.text or ""
    if not raw_text:
        return

    chat_id = message.chat.id
    user_id = message.from_user.id

    # ── Pengecualian: admin grup ──────────────────────────────────────────────
    if await is_admin(client, chat_id, user_id):
        return

    # ── Pengecualian: member VIP ──────────────────────────────────────────────
    if await free_col.find_one({"user_id": user_id, "chat_id": chat_id}):
        return

    # ── Rekam SELALU jalan untuk grup ini — independen dari toggle fitur
    #    lain manapun (local, global, dll), terlepas status toggle
    #    ubot_detect sendiri (lihat docstring modul). Fitur ini TIDAK
    #    boleh bergantung pada fitur lain yang ON/OFF. ─────────────────────
    from core.ubot_detect import record_sentence, evaluate_and_should_delete

    await record_sentence(chat_id, user_id, raw_text)

    # ── Evaluasi & eksekusi HANYA kalau fitur ini sendiri ON ─────────────────
    cfg = await get_config(chat_id)
    if not cfg.get("ubot_detect", False):
        return

    should_delete = await evaluate_and_should_delete(chat_id, user_id, raw_text)
    if not should_delete:
        return

    # ── Cek izin bot sebelum eksekusi — rekam sudah jalan di atas ───────────
    if not await check_bot_permissions(client, chat_id):
        return  # tidak punya izin hapus+ban → rekam sudah cukup, skip eksekusi

    # ── Terindikasi ubot & kalimat ini cocok salah satu kalimat ≥3× ──────────
    deleted = False
    try:
        await message.delete()
        deleted = True
    except (MessageDeleteForbidden, ChatAdminRequired):
        pass
    except FloodWait as fw:
        await asyncio.sleep(min(fw.value, 5))
        try:
            await message.delete()
            deleted = True
        except Exception:
            pass
    except Exception as e:
        print(f"[UbotDetect] Gagal hapus {message.id} di {chat_id}: {e}")

    # Ikut hitungan mute eskalasi terpusat (core/punishment.py) — sama
    # seperti jenis spam lain. Hanya diantrikan kalau delete benar2 terjadi,
    # konsisten dengan filter lain (bio.py, antispam.py, nexus_group.py).
    if deleted:
        asyncio.create_task(
            check_and_punish(client, message, "perilaku ubot (kalimat berulang)", raw_text[:100])
        )
        # Log ke panel per-grup
        asyncio.create_task(_log_ubot_deletion(client, message, raw_text))

    # StopPropagation di sini — bukan di finally — agar hanya jalan
    # saat kita memang memutuskan untuk memblokir pesan ini.
    raise StopPropagation


async def _log_ubot_deletion(client, message, raw_text: str) -> None:
    """Log aksi hapus ubot detect ke panel grup dan LOG_CHANNEL."""
    import os
    from plugins.commands.log import _send_log, _fmt_waktu, _user_line

    uid  = message.from_user.id
    cid  = message.chat.id
    name = message.from_user.first_name or str(uid)

    # Panel per-grup
    try:
        await insert_group_action_log(
            cid, "HAPUS",
            "Deteksi Ubot — kalimat berulang tanpa variasi",
            uid, name,
            raw_text[:100],
            jenis=_VIOLATION_UBOT,
        )
    except Exception:
        pass

    # LOG_CHANNEL
    LOG_CHANNEL = int(os.environ.get("LOG_CHANNEL", 0))
    if not LOG_CHANNEL:
        return

    user_mention = _user_line(uid, name)
    log_text = (
        f"<b>❖ {format_violation_header(_VIOLATION_UBOT)} ❖</b>\n"
        f"◈ <b>User:</b> {user_mention}\n"
        f"◈ <b>Grup:</b> {_html.escape(message.chat.title)} (<code>{cid}</code>)\n"
        f"◈ <b>Waktu:</b> {_fmt_waktu()}\n"
        f"◈ <b>Keterangan:</b> Terdeteksi sebagai ubot — mengirim kalimat yang sama berulang tanpa variasi\n\n"
        f"📨 <b>Pesan terakhir:</b>\n<code>{_html.escape(raw_text[:300])}</code>"
    )
    await _send_log(client, log_text)
