"""
core/pyrofork_compat.py
───────────────────────────────────────────────────────────────────────────────
Penerjemah error migrasi Pyrogram → Pyrofork.

TUJUAN:
  Saat sebuah parameter/atribut/method dihapus atau diganti nama di Pyrofork,
  Python akan melempar TypeError/AttributeError dengan pesan teknis seperti:
    "TypeError: Client.__init__() got an unexpected keyword argument 'xxx'"
    "AttributeError: 'Message' object has no attribute 'yyy'"
  Pesan ini SAH tapi tidak langsung memberi tahu apa penggantinya.

  Modul ini menerjemahkan pesan teknis itu menjadi saran yang lebih jelas,
  berdasarkan daftar MAPPING yang disusun manual dari perubahan API yang
  diketahui umum terjadi antara Pyrogram klasik dan Pyrofork.

‼️ KETERBATASAN PENTING — WAJIB DIBACA:
  Daftar MAPPING di bawah ini BUKAN hasil scan otomatis terhadap source code
  Pyrofork 2.3.69 (tidak ada akses internet/package registry dari environment
  pembuatan modul ini). Daftar ini disusun dari pengetahuan umum tentang
  breaking changes yang SERING dilaporkan terjadi pada Pyrogram-fork
  (Pyrofork, Pyrogram v2, dst). Artinya:
    • Daftar ini BISA TIDAK LENGKAP — error lain yang tidak ada di sini akan
      tetap muncul, hanya tanpa saran tambahan (raw traceback tetap tampil).
    • Daftar ini BISA SUDAH USANG jika Pyrofork mengubah lagi APInya setelah
      modul ini ditulis.
    • Untuk kepastian 100%, selalu cross-check dengan dokumentasi resmi
      Pyrofork: https://docs.pyrofork.mayuri.my.id/ atau changelog rilis di
      PyPI/GitHub Pyrofork.

CARA PAKAI:
  Modul ini dipasang sebagai exception hook global (lihat antigcast.py) DAN
  sebagai try/except pembungkus di titik-titik kritis (Client init, dst).
  Setiap kali exception tertangkap, translate_error() dipanggil — jika ada
  yang cocok di MAPPING, baris tambahan akan tampil di log SETELAH traceback
  asli (traceback asli TIDAK disembunyikan/dihapus).
"""

from __future__ import annotations

import re
import traceback


# ─────────────────────────────────────────────────────────────────────────────
# DAFTAR MAPPING — pola regex pesan error → (penjelasan, pengganti yang disarankan)
# Tiap entri: (regex_pattern, "penjelasan singkat", "saran pengganti")
# ─────────────────────────────────────────────────────────────────────────────
_KNOWN_CHANGES: list[tuple[str, str, str]] = [
    # ── Client.__init__ parameter yang sering berubah ──────────────────────
    (
        r"Client\.__init__\(\).*unexpected keyword argument ['\"]ipv6['\"]",
        "Parameter ipv6= pada Client() sudah tidak dipakai dengan cara yang sama.",
        "Cek apakah Pyrofork versi ini masih menerima ipv6=; jika tidak, hapus "
        "parameter ini dari pemanggilan Client(...).",
    ),
    (
        r"Client\.__init__\(\).*unexpected keyword argument ['\"]no_updates['\"]",
        "Parameter no_updates= mungkin sudah berganti nama.",
        "Coba ganti ke skip_updates= (nama yang dipakai beberapa versi Pyrofork).",
    ),
    (
        r"Client\.__init__\(\).*unexpected keyword argument ['\"]plugins['\"]",
        "Parameter plugins=dict(root=...) untuk auto-load plugin folder mungkin "
        "berubah format di versi Pyrofork ini.",
        "Cek dokumentasi Pyrofork soal plugin loading — bisa jadi sekarang butuh "
        "format dict berbeda atau perlu app.add_handler() manual sebagai gantinya.",
    ),
    # ── Filters yang sering berubah nama/parameter ───────────────────────────
    (
        r"command\(\).*unexpected keyword argument ['\"]case_sensitive['\"]",
        "Parameter case_sensitive= pada filters.command() mungkin sudah dihapus.",
        "Hapus parameter ini; Pyrofork mungkin sudah selalu case-insensitive "
        "secara default, atau gunakan filters.regex() sebagai gantinya.",
    ),
    # ── Message / Chat attribute yang sering berubah ────────────────────────
    (
        r"'Message' object has no attribute ['\"]chat_id['\"]",
        "Message.chat_id sudah tidak ada.",
        "Gunakan message.chat.id sebagai gantinya.",
    ),
    (
        r"'Message' object has no attribute ['\"]user_id['\"]",
        "Message.user_id sudah tidak ada.",
        "Gunakan message.from_user.id sebagai gantinya (cek from_user tidak None dulu).",
    ),
    (
        r"'Client' object has no attribute ['\"]me['\"]",
        "Client.me (atribut cache info bot) sudah tidak ada/berubah cara aksesnya.",
        "Gunakan 'await client.get_me()' sebagai gantinya (perlu di-await, bukan atribut).",
    ),
    # ── ChatMember / status enum ─────────────────────────────────────────────
    (
        r"module ['\"]pyrogram\.enums['\"] has no attribute ['\"]ChatMemberStatus['\"]",
        "Lokasi import ChatMemberStatus mungkin berubah.",
        "Coba 'from pyrogram.enums import ChatMemberStatus' tetap dipakai — jika "
        "tetap gagal, cek apakah Pyrofork memindahkannya ke pyrogram.types.",
    ),
    # ── send_message / parse_mode ────────────────────────────────────────────
    (
        r"send_message\(\).*unexpected keyword argument ['\"]reply_to_message_id['\"]",
        "Parameter reply_to_message_id= pada send_message() mungkin berganti nama.",
        "Coba ganti ke reply_parameters=ReplyParameters(message_id=...) — pola "
        "baru yang dipakai Bot API versi terbaru & beberapa fork.",
    ),
    # ── Topic / forum (relevan untuk gejala supergroup bertopik) ─────────────
    (
        r"'Message' object has no attribute ['\"]message_thread_id['\"]",
        "Message.message_thread_id (ID topik forum) tidak ditemukan.",
        "Pastikan versi Pyrofork yang terpasang sudah mendukung forum topics; "
        "cek juga apakah nama atributnya 'reply_to_top_message_id' di versi ini.",
    ),
    (
        r"'Chat' object has no attribute ['\"]is_forum['\"]",
        "Chat.is_forum (penanda grup bertopik) tidak ditemukan.",
        "Cek dokumentasi Pyrofork untuk nama atribut forum yang dipakai versi "
        "ini — beberapa fork memakai nama berbeda untuk fitur ini.",
    ),
    # ── Dispatcher / handler group internals ─────────────────────────────────
    (
        r"'Dispatcher' object has no attribute ['\"]groups['\"]",
        "Struktur internal app.dispatcher.groups (dipakai script debug kita) "
        "tidak ada di versi Pyrofork ini.",
        "Struktur dispatcher internal Pyrofork berbeda dari Pyrogram klasik — "
        "skip pengecekan ini, gunakan log [DEBUG-MIGRASI] import manual saja.",
    ),
]


def translate_error(exc: BaseException) -> str | None:
    """
    Coba cocokkan exception dengan daftar MAPPING di atas.
    Return None jika tidak ada yang cocok (tidak semua error ada di daftar ini —
    lihat keterbatasan di docstring modul).
    """
    msg = f"{type(exc).__name__}: {exc}"
    for pattern, penjelasan, saran in _KNOWN_CHANGES:
        if re.search(pattern, msg):
            return (
                f"[PYROFORK-COMPAT] Kemungkinan penyebab: {penjelasan}\n"
                f"[PYROFORK-COMPAT] Saran pengganti: {saran}"
            )
    return None


def log_exception_with_hint(exc: BaseException, context: str = "") -> None:
    """
    Print traceback ASLI (tidak disembunyikan) + saran terjemahan jika ada
    yang cocok di MAPPING. Dipanggil dari except block di titik-titik kritis.
    """
    prefix = f"[{context}] " if context else ""
    print(f"{prefix}❌ {type(exc).__name__}: {exc}")
    traceback.print_exc()
    hint = translate_error(exc)
    if hint:
        print(hint)
    else:
        print(
            "[PYROFORK-COMPAT] Tidak ada saran otomatis untuk error ini — "
            "daftar mapping di core/pyrofork_compat.py belum mencakup pola ini. "
            "Cek dokumentasi resmi Pyrofork secara manual untuk error di atas."
        )


def install_global_exception_hook() -> None:
    """
    Pasang hook global untuk exception yang tidak tertangkap (uncaught) di
    asyncio event loop — agar SEMUA crash startup/runtime ikut diterjemahkan,
    bukan hanya yang sudah dibungkus try/except manual.
    """
    import sys
    import asyncio

    def _sync_hook(exc_type, exc_value, exc_tb):
        log_exception_with_hint(exc_value, context="UNCAUGHT")
        sys.__excepthook__(exc_type, exc_value, exc_tb)

    sys.excepthook = _sync_hook

    def _asyncio_hook(loop, context):
        exc = context.get("exception")
        if exc is not None:
            log_exception_with_hint(exc, context="ASYNCIO-UNCAUGHT")
        else:
            loop.default_exception_handler(context)

    try:
        asyncio.get_event_loop().set_exception_handler(_asyncio_hook)
    except RuntimeError:
        pass  # Belum ada event loop saat modul ini diimpor — akan dipasang lagi nanti
