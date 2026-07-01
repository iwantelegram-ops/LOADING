"""
core/violation_types.py — Registry Jenis Pelanggaran (SATU sumber kebenaran)
════════════════════════════════════════════════════════════════════════════════
LATAR BELAKANG:
  Sebelum modul ini ada, tiap fungsi log (LOG_CHANNEL, LOG_OS, panel log per
  grup) menulis teks `alasan` BEBAS sendiri-sendiri, lalu panel grup harus
  MENEBAK jenis pelanggarannya lewat keyword-matching pada teks itu
  (lihat git history plugins/ui/pages.py — fungsi _alasan_icon lama).
  Hasilnya: 1 jenis pelanggaran bisa muncul dengan icon/label berbeda-beda
  tergantung dari mana ia dipanggil, dan jenis baru gampang tidak terdeteksi
  sama sekali oleh keyword-matching (jatuh ke ikon generik ⚠️).

DESAIN:
  Modul ini TIDAK punya dependency ke database.py / pyrogram / apapun —
  murni dict statis. Aman diimpor dari mana saja (database.py, log.py,
  video_call.py, pages.py, punishment.py, dst) tanpa risiko circular import.

  Tiap sumber pelanggaran (filter regex, bio, CAS, Nexus AI, mic VC, dst)
  WAJIB mengirim `kode` (salah satu konstanta VIOLATION_* di bawah) — bukan
  lagi teks alasan bebas — ke insert_group_action_log() / fungsi log lain.
  `kode` inilah yang disimpan di DB sebagai field "jenis" (field BARU,
  terpisah dari "alasan" yang tetap ada sebagai teks detail/konten bebas
  untuk ditampilkan apa adanya, mis. pola regex yang cocok).

  Icon + label Indonesia ramah HANYA didefinisikan SEKALI di VIOLATION_META
  di bawah — semua consumer (log.py, video_call.py, pages.py) memanggil
  get_violation_meta(kode) untuk mengambilnya. Tidak ada lagi keyword
  matching di tempat lain.

KATEGORI (dipakai untuk pengelompokan ringan, bukan dirender langsung):
  "pesan"   — pelanggaran terkait isi pesan/konten yang dihapus
  "profil"  — pelanggaran terkait profil (bio)
  "akun"    — tindakan terhadap akun (mute/ban/unadmin)
  "vc"      — terkait obrolan suara (mic mute/unmute oleh Security OS)
  "sistem"  — notifikasi sistem (grup baru, dll) — bukan pelanggaran user

BACKWARD COMPAT: entri log lama (sebelum modul ini ada) tidak punya field
  "jenis" di DB. get_violation_meta() menerima None/kode tak dikenal dan
  mengembalikan fallback (lihat _FALLBACK) — tidak pernah error/KeyError.
"""

from __future__ import annotations

# ── Kode jenis pelanggaran ──────────────────────────────────────────────────
# Konstanta ini yang dikirim sebagai `kode` — JANGAN dipakai untuk teks
# tampilan langsung, selalu lewat get_violation_meta()/get_violation_label().

VIOLATION_DUPLIKAT_LOKAL     = "DUPLIKAT_LOKAL"      # pesan duplikat berulang dalam 1 grup
VIOLATION_GCAST_GLOBAL       = "GCAST_GLOBAL"        # pesan sama disebar >1 grup
VIOLATION_BIO_LINK           = "BIO_LINK"            # link di bio profil
VIOLATION_MENTION_NON_MEMBER = "MENTION_NON_MEMBER"  # mention user bukan anggota grup
VIOLATION_LINK_PESAN         = "LINK_PESAN"          # link di dalam isi pesan
VIOLATION_REGEX_GLOBAL       = "REGEX_GLOBAL"        # filter kata global (owner)
VIOLATION_REGEX_GRUP         = "REGEX_GRUP"          # filter kata lokal grup
VIOLATION_NEXUS_AI           = "NEXUS_AI"            # dihapus oleh AI Nexus
VIOLATION_CAS_BAN            = "CAS_BAN"             # ban otomatis CAS
VIOLATION_MUTE_ESKALASI      = "MUTE_ESKALASI"       # 10x pelanggaran berturut → mute naik
VIOLATION_MUTE_GAGAL         = "MUTE_GAGAL"          # eksekusi mute gagal (izin bot kurang)
VIOLATION_BAN_GAGAL          = "BAN_GAGAL"           # eksekusi ban gagal (izin bot kurang)
VIOLATION_WHITELIST_SPARED   = "WHITELIST_SPARED"    # match pola TAPI tidak dihapus (whitelist)
VIOLATION_MUTE_SENYAP        = "MUTE_SENYAP"         # hapus senyap — user masih masa mute
VIOLATION_BIO_ADMIN_WAJIB    = "BIO_ADMIN_WAJIB"     # admin di-unadmin (bio tidak sesuai syarat)

# Mic VC (Security OS) — tetap dipisah dari pelanggaran "pesan" di atas,
# tapi pakai registry yang SAMA supaya konsisten lintas LOG_OS & panel.
VIOLATION_VC_MUTE_NON_MEMBER = "VC_MUTE_NON_MEMBER"  # mic mute: bukan member grup
VIOLATION_VC_MUTE_PEER       = "VC_MUTE_PEER"        # mic mute: profil belum terverifikasi
VIOLATION_VC_MUTE_BIO_LINK   = "VC_MUTE_BIO_LINK"    # mic mute: bio mengandung link
VIOLATION_VC_UNMUTE          = "VC_UNMUTE"           # mic dibuka kembali (bio sudah bersih)

# Bukan pelanggaran — notifikasi sistem (tetap pakai registry yang sama
# supaya format header/icon seragam dengan entri pelanggaran).
VIOLATION_SISTEM_GRUP_BARU   = "SISTEM_GRUP_BARU"
VIOLATION_SECOS_AKTIF        = "SECOS_AKTIF"
VIOLATION_SECOS_NONAKTIF     = "SECOS_NONAKTIF"


# ── Registry: kode → (icon, label Indonesia ramah, kategori) ───────────────
VIOLATION_META: dict[str, tuple[str, str, str]] = {
    VIOLATION_DUPLIKAT_LOKAL:     ("🔁", "Pesan Duplikat Berulang",            "pesan"),
    VIOLATION_GCAST_GLOBAL:       ("🌐", "Pesan Gcast (Sama di Banyak Grup)",  "pesan"),
    VIOLATION_BIO_LINK:           ("🔍", "Link di Bio",                       "profil"),
    VIOLATION_MENTION_NON_MEMBER: ("👤", "Mention User Bukan Anggota",        "pesan"),
    VIOLATION_LINK_PESAN:         ("🔗", "Link di Pesan",                     "pesan"),
    VIOLATION_REGEX_GLOBAL:       ("🚫", "Filter Kata Global",                "pesan"),
    VIOLATION_REGEX_GRUP:         ("🔡", "Filter Kata Grup",                  "pesan"),
    VIOLATION_NEXUS_AI:           ("🤖", "Dihapus oleh AI Nexus",             "pesan"),
    VIOLATION_CAS_BAN:            ("⛔", "Ban Otomatis (CAS)",                "akun"),
    VIOLATION_MUTE_ESKALASI:      ("🔇", "Mute Eskalasi (10× Berulang)",      "akun"),
    VIOLATION_MUTE_GAGAL:         ("⚠️", "Mute Gagal — Izin Bot Kurang",      "akun"),
    VIOLATION_BAN_GAGAL:          ("⚠️", "Ban Gagal — Izin Bot Kurang",       "akun"),
    VIOLATION_WHITELIST_SPARED:   ("✅", "Tidak Dihapus (Whitelist)",         "pesan"),
    VIOLATION_MUTE_SENYAP:        ("🔕", "Hapus Senyap (Masa Mute Aktif)",    "pesan"),
    VIOLATION_BIO_ADMIN_WAJIB:    ("👮", "Admin Di-unadmin (Bio Tidak Sesuai)", "akun"),

    VIOLATION_VC_MUTE_NON_MEMBER: ("🎙", "Mic Di-Mute — Bukan Anggota Grup",  "vc"),
    VIOLATION_VC_MUTE_PEER:       ("🎙", "Mic Di-Mute — Profil Belum Terverifikasi", "vc"),
    VIOLATION_VC_MUTE_BIO_LINK:   ("🎙", "Mic Di-Mute — Bio Mengandung Link", "vc"),
    VIOLATION_VC_UNMUTE:          ("🔊", "Mic Dibuka — Bio Sudah Bersih",     "vc"),

    VIOLATION_SISTEM_GRUP_BARU:   ("➕", "Bot Bergabung ke Grup Baru",         "sistem"),
    VIOLATION_SECOS_AKTIF:        ("🔐", "Security OS Diaktifkan",            "sistem"),
    VIOLATION_SECOS_NONAKTIF:     ("🔐", "Security OS Dinonaktifkan",         "sistem"),
}

# Fallback untuk kode tidak dikenal / data lama tanpa field "jenis" sama
# sekali (entri yang ditulis sebelum modul ini ada).
_FALLBACK: tuple[str, str, str] = ("⚠️", "Pelanggaran Lain", "pesan")


def get_violation_meta(kode: str | None) -> tuple[str, str, str]:
    """
    (icon, label, kategori) untuk `kode`. Tidak pernah raise — kode tak
    dikenal (termasuk None, dari data lama) jatuh ke _FALLBACK.
    """
    if not kode:
        return _FALLBACK
    return VIOLATION_META.get(kode, _FALLBACK)


def get_violation_icon(kode: str | None) -> str:
    return get_violation_meta(kode)[0]


def get_violation_label(kode: str | None) -> str:
    return get_violation_meta(kode)[1]


def format_violation_header(kode: str | None) -> str:
    """'🔗 Link di Pesan' — dipakai sebagai baris judul/Tipe di tiap log."""
    icon, label, _ = get_violation_meta(kode)
    return f"{icon} {label}"
