"""
plugins/ui/request_peer_flow.py
───────────────────────────────────────────────────────────────────────────────
Flow "Pasang di Grup" menggunakan RequestPeer button (KeyboardButtonRequestPeer).

Alur:
  1. User klik "➕ Pasang di Grup Saya" → tampil halaman perjanjian pengguna
  2. User klik "SETUJU" → bot kirim KeyboardButtonRequestPeer
     (Telegram otomatis minta user pilih grup & bot jadi admin)
  3. Bot terima service message → konfirmasi sukses → kembali ke page_start
"""

import asyncio
from pyrogram import Client, filters, raw
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
)
from pyrogram.enums import ParseMode

# FSM state: user_id yang sedang dalam alur perjanjian → menunggu service message
_pending_request_peer: dict[int, bool] = {}  # uid → True


_PERJANJIAN_TEXT = (
    "📜 <b>PERJANJIAN PENGGUNAAN BOT</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    "Sebelum memasang bot ini ke grup Anda, harap baca dan pahami ketentuan berikut:\n\n"

    "🔐 <b>Keamanan &amp; Privasi Data</b>\n"
    "Seluruh data grup — termasuk pesan yang terdeteksi sebagai spam, "
    "konfigurasi filter, dan aktivitas anggota — <b>hanya digunakan untuk "
    "keperluan perlindungan grup Anda</b>. Data tidak dijual, tidak dibagikan, "
    "dan tidak diakses oleh pihak manapun di luar sistem bot ini.\n\n"

    "⚙️ <b>Hak Akses Admin Bot</b>\n"
    "Bot memerlukan full hak admin:\n"
    "◈ <b>Hapus Pesan</b> — untuk menghapus spam secara otomatis\n"
    "◈ <b>Batasi Anggota</b> — untuk mute/ban akun terverifikasi berbahaya\n"
    "◈ <b>Dan Semua Ijin Admin</b> — untuk menjalankan seluruh fiturnya\n\n"
    "Semua hak ini <b>semata-mata digunakan untuk fungsi perlindungan</b>.\n"
    "Bot tidak akan pernah menggunakannya untuk kepentingan lain.\n\n"

    "🛡️ <b>Jaminan Integritas</b>\n"
    "Pemilik bot berkomitmen untuk <b>tidak mencampuri urusan internal grup</b> "
    "dalam bentuk apapun. Tidak ada penyadapan, tidak ada penyalahgunaan izin, "
    "dan tidak ada akses yang melebihi kebutuhan teknis bot.\n\n"

    "✅ Dengan menekan <b>SETUJU</b>, Anda menyatakan telah membaca, memahami, "
    "dan menyetujui seluruh ketentuan di atas.\n\n"
    "<i>Anda dapat mencabut izin bot kapan saja melalui pengaturan admin grup.</i>"
)


async def show_perjanjian(client: Client, message: Message) -> None:
    """
    Tampilkan halaman perjanjian pengguna.
    Teks perjanjian dikirim sebagai pesan baru dengan ReplyKeyboard di bawah —
    tombol SETUJU adalah bottom button, bukan inline.
    """
    # Hapus inline keyboard dari pesan sebelumnya
    try:
        await message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    # Kirim teks perjanjian dengan tombol SETUJU sebagai ReplyKeyboard (bottom)
    # dan tombol Batal sebagai ReplyKeyboard juga
    reply_keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton("✅ SETUJU — Pasang Bot ke Grup")],
            [KeyboardButton("❌ Batal")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await client.send_message(
        message.chat.id,
        _PERJANJIAN_TEXT,
        reply_markup=reply_keyboard,
        parse_mode=ParseMode.HTML,
    )


async def send_request_peer_button(client: Client, uid: int) -> None:
    """
    Setelah user setuju — kirim ReplyKeyboard dengan KeyboardButtonRequestPeer
    agar Telegram tampilkan dialog pilih grup.
    uid: telegram user_id yang akan menerima button ini.
    """
    _pending_request_peer[uid] = True

    # Hak yang diminta untuk bot di grup — DISAMAKAN PERSIS dengan script
    # contoh yang sudah terbukti jalan (semua True), supaya tidak ada lagi
    # variabel pembeda saat debugging USER_RIGHTS_MISSING. Bisa dipersempit
    # lagi nanti setelah alur dasar ini terbukti berhasil.
    bot_rights = raw.types.ChatAdminRights(
        change_info=True,
        delete_messages=True,
        ban_users=True,
        invite_users=True,
        pin_messages=True,
        add_admins=True,
        anonymous=False,
        manage_call=True,
    )

    # Syarat sinkronisasi untuk user yang mengklik tombol — disamakan persis juga
    user_rights = raw.types.ChatAdminRights(
        change_info=True,
        delete_messages=True,
        ban_users=True,
        invite_users=True,
        pin_messages=True,
        add_admins=True,
        anonymous=False,
        manage_call=True,
    )

    raw_button = raw.types.KeyboardButtonRequestPeer(
        text="👥 Pilih Grup untuk Dipasangi Bot",
        button_id=101,
        max_quantity=1,
        peer_type=raw.types.RequestPeerTypeChat(
            user_admin_rights=user_rights,
            bot_admin_rights=bot_rights,
        ),
    )

    # Kirim via raw invoke — persis pola script contoh yang sudah terbukti
    # jalan di environment ini (Pyrofork, nama paket "pyrogram").
    await client.invoke(
        raw.functions.messages.SendMessage(
            peer=await client.resolve_peer(uid),
            message="👇 Pilih grup yang ingin dipasangi bot. Bot akan otomatis menjadi admin dengan izin yang diperlukan. Telegram akan meminta konfirmasi sebelum melanjutkan.",
            random_id=client.rnd_id(),
            reply_markup=raw.types.ReplyKeyboardMarkup(
                rows=[raw.types.KeyboardButtonRow(buttons=[raw_button])],
                resize=True,
            ),
        )
    )


async def handle_request_peer_result(client: Client, message: Message) -> bool:
    """
    Tangkap service message hasil RequestPeer.
    Return True jika berhasil diproses (caller bisa stop propagation).
    """
    uid = message.from_user.id if message.from_user else None
    if not uid or uid not in _pending_request_peer:
        return False

    # Coba ekstrak chat_id dari action
    try:
        action = getattr(message, "action", None)
        if action is None and hasattr(message, "raw"):
            action = getattr(message.raw, "action", None)
        if action is None:
            return False

        peer = None
        if hasattr(action, "peers") and action.peers:
            peer = action.peers[0]
        elif hasattr(action, "peer"):
            peer = action.peer

        if peer is None:
            return False

        if hasattr(peer, "chat_id"):
            real_id = int(f"-{peer.chat_id}")
        elif hasattr(peer, "channel_id"):
            real_id = int(f"-100{peer.channel_id}")
        else:
            return False

    except Exception as e:
        print(f"[RequestPeer] Gagal parse peer: {e}")
        return False

    _pending_request_peer.pop(uid, None)

    # Hapus ReplyKeyboard
    try:
        await client.send_message(
            uid,
            "⏳ Memproses...",
            reply_markup=ReplyKeyboardRemove(),
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass

    # Tampilkan hasil
    try:
        from pyrogram.errors import ChatAdminRequired, UserNotParticipant
        try:
            chat = await client.get_chat(real_id)
            chat_title = chat.title or str(real_id)
        except Exception:
            chat_title = str(real_id)

        # Kembali ke page_start
        from plugins.ui.pages import page_start
        text, keyboard = await page_start(client)
        success_text = (
            f"✅ <b>Bot berhasil dipasang di grup!</b>\n\n"
            f"🏷 Grup: <b>{chat_title}</b>\n"
            f"🆔 ID: <code>{real_id}</code>\n\n"
            f"Bot sudah aktif sebagai admin dengan izin yang diperlukan.\n"
            f"Ketik /antigcast di grup tersebut untuk membuka panel pengaturan.\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        ) + text

        await client.send_message(
            uid,
            success_text,
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception as e:
        print(f"[RequestPeer] Gagal kirim konfirmasi: {e}")
        await client.send_message(uid, f"✅ Bot berhasil dipasang ke grup <code>{real_id}</code>.", parse_mode=ParseMode.HTML)

    return True
