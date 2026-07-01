"""
core/ubot_detect.py — Deteksi Ubot (pengganti fitur lama "Agresif Spam")
════════════════════════════════════════════════════════════════════════════════
KONSEP:
  Tiap user di tiap grup punya "populasi kalimat" sendiri — daftar kalimat
  RAW (teks asli, tanpa normalisasi apapun) yang pernah dia kirim di grup
  itu, masing-masing dengan hitungan berapa kali kalimat itu (100% identik)
  terkirim ulang. Kalimat yang sama persis TIDAK disimpan berkali-kali —
  hanya 1 entri dengan counter bertambah.

  ATURAN STATUS "PERILAKU UBOT" (per USER, per GRUP):
    - Ambil semua kalimat user itu di grup itu yang masih berlaku (TTL belum
      habis).
    - Kalau ADA minimal 1 kalimat dengan count < 3  → user AMAN. Semua
      kalimat (termasuk yang sudah ≥3×) tetap diloloskan ke proses
      keamanan lain (regex, dll) — TIDAK ada yang dihapus oleh modul ini.
    - Kalau SEMUA kalimat tercatat sudah ≥3× (dan minimal ada 1 kalimat
      tercatat sama sekali) → user dianggap berperilaku ubot. Pesan
      BERIKUTNYA yang PERSIS COCOK salah satu kalimat tercatat → dilempar
      ke worker hapus oleh pemanggil (lihat plugins/filters/ubot_detect_
      filter.py). Pesan dengan kalimat yang BELUM PERNAH tercatat (kalimat
      baru) tetap direkam (jadi count=1) — begitu direkam, kondisi "semua
      kalimat ≥3×" otomatis gugur untuk evaluasi pesan SETELAHNYA, sehingga
      user itu balik "aman" lagi sampai semua variasi barunya juga ≥3×.

  REKAM BERJALAN TERLEPAS STATUS TOGGLE FITUR INI:
    record_sentence() WAJIB dipanggil untuk setiap pesan teks di grup yang
    punya MINIMAL SATU fitur bot aktif — bukan hanya saat ubot_detect=True.
    Ini supaya begitu owner menyalakan fitur ini di kemudian hari, data
    riwayat 7 hari ke belakang sudah tersedia, bukan mulai dari nol.
    Keputusan "hapus atau tidak" (evaluate_and_should_delete) yang HARUS
    tetap dicek statusnya — itu baru jalan kalau ubot_detect=True.

TTL 7 HARI, DI-REFRESH PER KEMUNCULAN:
    Tiap kali kalimat yang SAMA muncul lagi, expires_at di-set ulang jadi
    (now + 7 hari) — bukan dihitung dari kemunculan pertama. Memakai
    MongoDB native TTL index (expireAfterSeconds=0 pada field expires_at)
    sehingga penghapusan otomatis dilakukan MongoDB sendiri, tidak perlu
    cron job manual di sisi aplikasi.

BATAS MAKSIMUM KALIMAT (UBOT_MAX_SENTENCES = 50):
    Maksimal 50 kalimat unik tersimpan per user per grup. Begitu kalimat
    baru ke-51 masuk, kalimat dengan expires_at paling awal (paling lama
    tidak diulang — LRU) dihapus otomatis. 50 dokumen kecil per user sangat
    ringan untuk index {chat_id, user_id} — bukan masalah performa.

MEMORI TERPISAH DARI "ANTI DUPLIKASI LOKAL":
    Modul ini memakai collection Mongo SENDIRI (ubot_sentence_tracker).
    Fitur "Anti Duplikasi Lokal" (plugins/filters/antispam.py +
    core/antispam_queue.py) memakai collection BERBEDA (seen_messages,
    type="local_track"). Dua fitur ini independen total — tidak ada
    field, cache, atau gate yang dibagi antara keduanya. ubot_detect
    TIDAK PERNAH bergantung pada toggle "local" (atau toggle fitur
    lain manapun) untuk merekam — hanya bergantung pada toggle dirinya
    sendiri (ubot_detect) untuk evaluasi & eksekusi hapus.

    ubot_sentence_tracker terdaftar di core/mongo_shard.py
    SHARDED_COLLECTIONS, sehingga ikut skema multi-cluster MongoDB yang
    sama dengan seen_messages — data didistribusikan per chat_id ke
    cluster yang sesuai, dengan fallback SQLite otomatis per-shard.

INDEX:
    - {chat_id, user_id, sentence_hash} unique  → upsert cepat per kalimat.
    - {chat_id, user_id}                        → ambil semua kalimat 1 user
                                                    dengan cepat saat evaluasi.
    - {expires_at} TTL                          → auto-expire native MongoDB.
"""

from __future__ import annotations

import time
import hashlib
from datetime import datetime, timedelta, timezone

UBOT_TTL_DAYS      = 7
UBOT_COUNT_THRESHOLD = 3   # kalimat dianggap "matang" begitu count >= ini
UBOT_MAX_SENTENCES   = 50  # maksimal kalimat unik tersimpan per user per grup

_ttl_index_created = False


def _sentence_hash(raw_text: str) -> str:
    """
    Hash dari teks RAW APA ADANYA — tidak ada normalisasi (lowercase, strip,
    dst) sama sekali. "100% sama" berarti byte-identik, sesuai permintaan:
    kalimat asli (raw), bukan kalimat yang sudah dipreproses.
    """
    return hashlib.sha256(raw_text.encode("utf-8", errors="ignore")).hexdigest()


def _ubot_col():
    """Lazy import supaya modul ini tidak punya circular import ke database.py."""
    from database import db
    return db["ubot_sentence_tracker"]


async def ensure_ubot_detect_index() -> None:
    """
    Buat index untuk ubot_sentence_tracker. Idempotent — aman dipanggil
    tiap startup (lihat pola ensure_mention_cache_index di database.py).
    """
    global _ttl_index_created
    if _ttl_index_created:
        return
    try:
        from database import _BACKEND
        if _BACKEND != "mongo":
            _ttl_index_created = True
            return
    except Exception:
        pass

    try:
        col = _ubot_col()
        await col.create_index("expires_at", expireAfterSeconds=0)
        await col.create_index(
            [("chat_id", 1), ("user_id", 1), ("sentence_hash", 1)], unique=True
        )
        await col.create_index([("chat_id", 1), ("user_id", 1)])
        _ttl_index_created = True
        print("[UbotDetect] ✅ Index ubot_sentence_tracker siap.")
    except Exception as e:
        print(f"[UbotDetect] ⚠️  Gagal buat index: {e}")


async def record_sentence(chat_id: int, user_id: int, raw_text: str) -> None:
    """
    Catat 1 kemunculan kalimat RAW dari user ini di grup ini.
    - Kalimat baru               → insert dengan count=1.
    - Kalimat sudah pernah ada   → count += 1, expires_at di-refresh ke
                                    (now + 7 hari) dari kemunculan TERBARU ini.

    BATAS MAKSIMUM: maksimal UBOT_MAX_SENTENCES (50) kalimat unik tersimpan
    per user per grup. Begitu kalimat BARU (insert, bukan update existing)
    membuat total melebihi batas, kalimat dengan expires_at PALING AWAL
    (paling lama tidak diulang — karena TTL di-refresh tiap kemunculan, ini
    otomatis berarti "paling lama tidak diulang" = LRU) dihapus untuk
    membuat slot. 50 dokumen kecil per user adalah ukuran yang sangat
    ringan untuk index {chat_id, user_id} — tidak ada masalah performa.

    Dipanggil untuk SEMUA pesan teks selama minimal 1 fitur bot aktif di
    grup ini — TERLEPAS status toggle ubot_detect sendiri (lihat docstring
    modul). Tidak melempar exception ke pemanggil (non-fatal, fail-silent
    ke log saja) supaya kegagalan DB tidak pernah menghalangi alur filter
    lain yang memanggil ini di awal pipeline.
    """
    if not raw_text:
        return
    try:
        col   = _ubot_col()
        now   = time.time()
        h     = _sentence_hash(raw_text)
        until = datetime.now(timezone.utc) + timedelta(days=UBOT_TTL_DAYS)

        existing = await col.find_one(
            {"chat_id": chat_id, "user_id": user_id, "sentence_hash": h}
        )
        is_new = existing is None

        await col.update_one(
            {"chat_id": chat_id, "user_id": user_id, "sentence_hash": h},
            {
                "$set": {
                    "chat_id": chat_id,
                    "user_id": user_id,
                    "sentence_hash": h,
                    "raw_text": raw_text,
                    "last_seen": now,
                    "expires_at": until,
                },
                "$setOnInsert": {"first_seen": now},
                "$inc": {"count": 1},
            },
            upsert=True,
        )

        if is_new:
            await _evict_if_over_limit(col, chat_id, user_id)
    except Exception as e:
        print(f"[UbotDetect] Gagal record_sentence chat={chat_id} uid={user_id}: {e}")


async def _evict_if_over_limit(col, chat_id: int, user_id: int) -> None:
    """
    Kalau jumlah kalimat user ini > UBOT_MAX_SENTENCES, hapus entri dengan
    expires_at PALING AWAL (LRU) sampai pas di batas. Biasanya hanya 1
    entri yang perlu dihapus per panggilan (insert 1-per-1), tapi loop
    dijaga untuk keamanan kalau ada anomali (mis. migrasi data lama).
    """
    try:
        cursor = col.find({"chat_id": chat_id, "user_id": user_id})
        docs = [doc async for doc in cursor]
        if len(docs) <= UBOT_MAX_SENTENCES:
            return

        docs.sort(key=lambda d: d.get("expires_at") or 0)
        n_to_evict = len(docs) - UBOT_MAX_SENTENCES
        for doc in docs[:n_to_evict]:
            try:
                await col.delete_one({
                    "chat_id": chat_id,
                    "user_id": user_id,
                    "sentence_hash": doc.get("sentence_hash"),
                })
            except Exception as e:
                print(f"[UbotDetect] Gagal evict kalimat lama chat={chat_id} uid={user_id}: {e}")
    except Exception as e:
        print(f"[UbotDetect] Gagal cek batas kalimat chat={chat_id} uid={user_id}: {e}")


async def evaluate_and_should_delete(chat_id: int, user_id: int, raw_text: str) -> bool:
    """
    WAJIB dipanggil SETELAH record_sentence() untuk pesan yang sama (lihat
    plugins/filters/ubot_detect_filter.py — record dulu, baru evaluate, supaya
    kalimat yang baru saja masuk juga ikut diperhitungkan dalam evaluasi
    "semua kalimat user ini ≥3×?").

    Return True jika user ini terindikasi berperilaku ubot (SEMUA kalimat
    miliknya di grup ini sudah ≥3× tanpa ada satupun yang masih < 3×) DAN
    raw_text pesan ini SENDIRI juga termasuk kalimat yang sudah ≥3× —
    artinya pesan ini cocok salah satu "kalimat archive" yang berulang.

    False jika:
      - Ada minimal 1 kalimat user ini yang count < 3 (user masih "aman").
      - Tidak ada kalimat tercatat sama sekali (mustahil secara praktis
        karena record_sentence() sudah dipanggil duluan, tapi dijaga untuk
        keamanan).
    """
    try:
        col = _ubot_col()
        cursor = col.find({"chat_id": chat_id, "user_id": user_id})
        sentences = [doc async for doc in cursor]

        if not sentences:
            return False

        # Kalau ADA satu saja yang belum matang (< 3x) → user aman SELURUHNYA.
        if any(doc.get("count", 0) < UBOT_COUNT_THRESHOLD for doc in sentences):
            return False

        # Semua kalimat sudah ≥3× — user terindikasi ubot. Pastikan kalimat
        # PESAN INI SENDIRI memang salah satu yang tercatat ≥3× (harus selalu
        # benar di titik ini karena record_sentence() barusan menambah count
        # kalimat ini juga, tapi dicek ulang demi keamanan/kejelasan logika).
        h = _sentence_hash(raw_text)
        for doc in sentences:
            if doc.get("sentence_hash") == h and doc.get("count", 0) >= UBOT_COUNT_THRESHOLD:
                return True

        return False
    except Exception as e:
        print(f"[UbotDetect] Gagal evaluate chat={chat_id} uid={user_id}: {e}")
        # Fail-safe: kalau evaluasi gagal, JANGAN hapus pesan siapapun.
        return False


async def any_feature_active(chat_id: int) -> bool:
    """
    [TIDAK DIPAKAI SEBAGAI GATE record_sentence() LAGI]
    record_sentence() di plugins/filters/ubot_detect_filter.py SEKARANG
    SELALU dipanggil untuk grup ini, independen dari fungsi ini — supaya
    deteksi ubot tidak pernah bergantung pada toggle fitur lain manapun
    (termasuk "local"). Fungsi ini disisakan sebagai utilitas diagnostik/
    panel saja, BUKAN gate produksi.

    True jika MINIMAL SATU fitur bot (apapun) aktif di grup ini.

    Mencakup:
      - Field boolean utama di DEFAULT_CONFIG (local, global, bio_check,
        anti_mention, anti_link, cas, anti_spam_ai, ubot_detect).
      - Security OS (cek terpisah, collection berbeda).
      - NewsCore (cek terpisah, collection berbeda).
    """
    try:
        from database import get_config
        cfg = await get_config(chat_id)
        basic_flags = (
            cfg.get("local", False),
            cfg.get("global", False),
            cfg.get("bio_check", False),
            cfg.get("anti_mention", False),
            cfg.get("anti_link", False),
            cfg.get("cas", False),
            cfg.get("anti_spam_ai", False),
            cfg.get("ubot_detect", False),
        )
        if any(basic_flags):
            return True
    except Exception as e:
        print(f"[UbotDetect] any_feature_active: gagal cek config dasar: {e}")

    try:
        from video_call import security_os_get_status
        sec_doc = await security_os_get_status(chat_id)
        if sec_doc.get("enabled", False):
            return True
    except Exception:
        pass  # Security OS module mungkin tidak tersedia — tidak fatal

    try:
        from database import ns_get_config
        ns_cfg = await ns_get_config(chat_id)
        if ns_cfg.get("enabled", False):
            return True
    except Exception:
        pass

    return False


async def get_user_sentence_summary(chat_id: int, user_id: int) -> dict:
    """
    Ringkasan untuk keperluan panel/debug: jumlah kalimat tercatat, berapa
    yang sudah matang (≥3×), dan apakah user ini SEDANG terindikasi ubot.
    Tidak dipakai di hot-path filter — hanya untuk UI/command diagnostik.
    """
    try:
        col = _ubot_col()
        cursor = col.find({"chat_id": chat_id, "user_id": user_id})
        sentences = [doc async for doc in cursor]
        total   = len(sentences)
        matang  = sum(1 for d in sentences if d.get("count", 0) >= UBOT_COUNT_THRESHOLD)
        is_ubot = total > 0 and matang == total
        return {
            "total_kalimat": total,
            "kalimat_matang": matang,
            "terindikasi_ubot": is_ubot,
        }
    except Exception as e:
        print(f"[UbotDetect] Gagal get_user_sentence_summary: {e}")
        return {"total_kalimat": 0, "kalimat_matang": 0, "terindikasi_ubot": False}
