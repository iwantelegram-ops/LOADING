"""
core/vip_bio_guard.py
──────────────────────
Penegakan "VIP Bio" — auto-masuk dan auto-keluar VIP berdasarkan teks
di bio profil Telegram user, per grup.

KONSEP:
  Owner/admin grup bisa set sebuah teks (via panel Bio → "Atur Teks VIP
  Bio"). Setiap user yang bio profilnya MENGANDUNG teks itu (substring,
  case-insensitive) otomatis dianggap Member VIP grup ini — didaftarkan
  ke collection free_per_group, dan SEKETIKA itu juga bebas dari SEMUA
  pengecekan bot di grup ini:
    - Bot utama   : antispam.py, bio.py (link), cas.py, nexus_group.py
    - Bot pemantau: hanya menulis hasil bio ke DB, tidak menindak —
                    sudah otomatis "tidak relevan" begitu user VIP karena
                    filter pemroses hasilnya (bio.py) skip duluan.
    - Userbot VC  : security_os/video_call.py (_is_vip_user)
  Semua titik itu membaca collection free_per_group YANG SAMA, jadi cukup
  modul ini yang menulis/menghapus dokumennya — tidak perlu mengubah
  filter lain satu per satu.

MASUK VIP (reaktif, real-time):
  Dipanggil oleh plugins/filters/bio.py setiap kali bio user dibaca
  (saat pesan masuk, saat typing, atau saat dipaksa fresh-check) DAN
  teks VIP ditemukan di bio, padahal user belum tercatat VIP via jalur
  ini. Begitu terdeteksi:
    1. Insert ke free_per_group dengan source="bio_vip" (PENTING — beda
       dari VIP manual /vip yang TIDAK punya field "source". Field ini
       memastikan auto-keluar VIP nanti HANYA menghapus entri yang
       memang dibuat otomatis oleh teks bio, tidak pernah menyentuh VIP
       yang sengaja diberikan admin secara manual).
    2. Invalidasi cache VIP userbot (video_call.invalidate_vip_cache)
       agar userbot langsung tahu status VIP terbaru.
    3. PRIORITAS: jika user sedang dalam masa mute/restrict —
       - Bot utama (local mute): reset counter + queue_unmute() agar
         restrict di Telegram benar-benar dibuka, bukan cuma dianggap
         "lewat" di pesan berikutnya.
       - Userbot VC (mic-mute Security OS): antri scan VC supaya userbot
         naik dan unmute mic user ini.
       Kedua unmute ini best-effort (fire-and-forget) — gagal tidak
       menggagalkan status VIP itu sendiri.
  Tidak ada notifikasi apapun ke grup atau ke user — sesuai permintaan.

KELUAR VIP (polling berkala, 10 menit per grup):
  Dipanggil oleh vip_bio_checker_loop() (loop background, didaftarkan di
  antigcast.py). Untuk setiap grup yang bio_check=True dan bio_vip_text
  terisi, untuk setiap entri free_per_group dengan source="bio_vip" di
  grup itu:
    1. Force-check bio fresh via bot pemantau (force_check_user).
       Jika bot pemantau grup ini tidak aktif / gagal fetch (None) →
       FAIL-SAFE: user TETAP VIP, coba lagi di siklus berikutnya. Tidak
       pernah mengeluarkan VIP berdasarkan data yang tidak fresh/basi.
    2. Jika teks VIP TIDAK LAGI ada di bio (hasil fresh, bukan basi)
       → hapus dari free_per_group, invalidasi cache VIP.
    3. TIDAK ada notifikasi apapun. TIDAK ada masa tenang — pesan
       berikutnya dari user ini langsung kena cek filter normal seperti
       biasa (bio link, regex, dll), sesuai keputusan desain.
  VIP manual (/vip, tanpa field "source" atau source != "bio_vip") TIDAK
  PERNAH disentuh oleh proses ini — auto-keluar hanya berlaku untuk VIP
  yang memang masuk lewat teks bio.

CATATAN ARSITEKTUR:
  Modul ini TIDAK fetch bio sendiri — selalu lewat bot pemantau
  (monitor_bot_reference.force_check_user), sama seperti bio.py dan
  ns_bio_guard.py. Tidak ada API tambahan ke Telegram di luar yang
  sudah ada.
"""

from __future__ import annotations

import asyncio
import time

from database import db, get_config, reset_local_mute, config_db, ns_is_current_admin
from core.member_tag import set_chat_member_tag

free_col = db["free_per_group"]
bio_col  = db["bio_profiles"]

# ── Throttle ringan: cegah insert/cek berulang untuk user+grup yang sama
# dalam waktu singkat (mis. beberapa pesan beruntun sebelum proses masuk-VIP
# pertama selesai). Bukan pengganti TTL bio_profiles — hanya pengaman lokal.
_entering: set[tuple[int, int]] = set()


# ─────────────────────────────────────────────────────────────────────────────
#  TITLE VIP — pasang/hapus tag member untuk SEMUA Member VIP (manual & bio)
# ─────────────────────────────────────────────────────────────────────────────
#
# Beda dari Auto Title Member NewsCore (rank-based, periodik tiap reset):
# Title VIP dipasang/dicabut SEKETIKA mengikuti status VIP itu sendiri —
# begitu user masuk VIP (lewat /vip, panel, atau teks bio) tag langsung
# terpasang; begitu status VIP hilang (unvip, hapus panel, atau teks bio
# hilang) tag langsung dicabut. Tidak menunggu periode reset apapun.
#
# Konsisten dengan Auto Title Member: dipasang via Bot API setChatMemberTag
# (core/member_tag.py), BUKAN set_administrator_title (member VIP bukan admin).

async def apply_vip_title(chat_id: int, user_id: int) -> None:
    """
    Pasang tag Title VIP ke satu user, jika fitur aktif & teks sudah diisi.

    Dipanggil tepat setelah user resmi tercatat VIP (insert ke free_per_group)
    dari jalur manapun: /vip, panel "Tambah Member VIP", atau auto-masuk
    via teks bio. Aman dipanggil berulang (no-op jika fitur OFF/teks kosong).
    """
    try:
        cfg = await get_config(chat_id)
        if not cfg.get("vip_title_enabled"):
            return
        tag = (cfg.get("vip_title") or "").strip()
        if not tag:
            return

        success, reason = await set_chat_member_tag(chat_id, user_id, tag)
        if success:
            from database import vip_set_titled_member
            await vip_set_titled_member(chat_id, user_id, tag)
        else:
            print(f"[VIP-Title] gagal pasang tag uid={user_id} chat={chat_id}: {reason}")
    except Exception as e:
        print(f"[VIP-Title] apply_vip_title error uid={user_id} chat={chat_id}: {e}")


async def clear_vip_title(chat_id: int, user_id: int) -> None:
    """
    Hapus tag Title VIP dari satu user (setChatMemberTag tag="").

    Dipanggil tepat setelah status VIP user ini dicabut dari jalur manapun:
    /unvip, panel "Unvip", auto-keluar via teks bio hilang, atau saat VIP
    dicabut paksa karena user jadi admin NewsCore. Aman dipanggil walau
    user tidak pernah punya tag VIP sama sekali (no-op jika tidak tercatat).
    """
    try:
        from database import vip_get_titled_members, vip_remove_titled_member
        titled = await vip_get_titled_members(chat_id)
        if not any(m["user_id"] == user_id for m in titled):
            return  # tidak pernah dipasangi tag VIP — tidak perlu hit API
        success, reason = await set_chat_member_tag(chat_id, user_id, "")
        if not success:
            print(f"[VIP-Title] gagal hapus tag uid={user_id} chat={chat_id}: {reason}")
        # Tetap hapus dari catatan walau API gagal — mencegah retry tanpa
        # henti untuk user yang sudah tidak VIP (sama seperti pola Auto
        # Title Member: kegagalan API tidak boleh membuat state nyangkut).
        await vip_remove_titled_member(chat_id, user_id)
    except Exception as e:
        print(f"[VIP-Title] clear_vip_title error uid={user_id} chat={chat_id}: {e}")


async def sync_vip_titles(chat_id: int) -> str:
    """
    Sinkronkan ulang tag Title VIP SEMUA Member VIP grup ini terhadap
    konfigurasi (vip_title_enabled + vip_title) SAAT INI dipanggil.

    Dipakai saat:
      - Owner mengubah/mengisi teks Title VIP baru → pasang ulang ke semua
        VIP existing dengan teks baru (bukan hanya yang baru masuk VIP
        setelahnya).
      - Owner menonaktifkan Title VIP → bersihkan semua tag yang nyangkut.

    Returns ringkasan singkat (string) untuk ditampilkan ke owner.
    """
    try:
        cfg     = await get_config(chat_id)
        enabled = cfg.get("vip_title_enabled", False)
        tag     = (cfg.get("vip_title") or "").strip()

        vip_docs = await free_col.find({"chat_id": chat_id}).to_list(length=2000)
        vip_ids  = [d["user_id"] for d in vip_docs if "user_id" in d]

        if not enabled or not tag:
            # Fitur OFF / teks kosong → bersihkan semua tag VIP yang masih nyangkut
            from database import vip_get_titled_members
            old_titled = await vip_get_titled_members(chat_id)
            cleared = 0
            for idx, m in enumerate(old_titled):
                if idx > 0:
                    await asyncio.sleep(0.5)
                await clear_vip_title(chat_id, m["user_id"])
                cleared += 1
            return f"{cleared} tag VIP lama dibersihkan." if cleared else ""

        ok_count = 0
        for idx, uid in enumerate(vip_ids):
            if idx > 0:
                await asyncio.sleep(0.5)
            await apply_vip_title(chat_id, uid)
            ok_count += 1
        return f"{ok_count} Member VIP ditandai dengan tag terbaru."
    except Exception as e:
        print(f"[VIP-Title] sync_vip_titles error chat={chat_id}: {e}")
        return ""


async def maybe_enter_vip_bio(client, chat_id: int, user_id: int) -> bool:
    """
    Dipanggil oleh bio.py setiap kali bio user terbukti mengandung teks
    VIP grup ini. Jika user belum VIP (lewat jalur apapun) → daftarkan
    sebagai VIP bio + langsung unmute (bot utama & userbot VC) jika perlu.

    Return True jika user baru saja didaftarkan sebagai VIP bio di
    pemanggilan ini (berguna untuk logging di pemanggil jika perlu),
    False jika user sudah VIP sebelumnya (tidak ada perubahan).
    """
    key = (chat_id, user_id)
    if key in _entering:
        return False

    try:
        existing = await free_col.find_one({"user_id": user_id, "chat_id": chat_id})
        if existing is not None:
            # Sudah VIP (entah lewat bio atau manual) — tidak ada yang perlu
            # dilakukan. Tidak menulis ulang/mengubah source yang sudah ada.
            return False

        # ── Larang admin NewsCore masuk VIP lewat teks bio ──────────────────
        # Kalau teks VIP Bio kebetulan (atau disengaja) sama/cocok dengan
        # teks "Bio Admin Wajib" NewsCore, admin NewsCore bisa otomatis
        # lolos jadi VIP — dan begitu VIP, dia bebas dari semua filter,
        # termasuk efek praktis dari bio guard NewsCore. Untuk mencegah ini,
        # admin NewsCore TIDAK PERNAH didaftarkan VIP lewat jalur otomatis
        # ini, apapun isi bionya.
        if await ns_is_current_admin(chat_id, user_id):
            return False

        _entering.add(key)
        try:
            await free_col.update_one(
                {"user_id": user_id, "chat_id": chat_id},
                {"$set": {
                    "user_id": user_id,
                    "chat_id": chat_id,
                    "source":  "bio_vip",
                    "since":   time.time(),
                }},
                upsert=True,
            )
        finally:
            _entering.discard(key)

        print(f"[VIP-Bio] uid={user_id} chat={chat_id} → MASUK VIP (teks ditemukan di bio).")

        # ── Title VIP: pasang tag otomatis jika fitur aktif ─────────────────
        asyncio.create_task(apply_vip_title(chat_id, user_id))

        # ── Invalidasi cache VIP userbot agar status baru langsung terbaca ──
        try:
            from video_call import invalidate_vip_cache
            invalidate_vip_cache(chat_id, user_id)
        except Exception:
            pass

        # ── PRIORITAS: buka mute yang sedang aktif, di kedua sisi ───────────
        asyncio.create_task(_unmute_everywhere(client, chat_id, user_id))

        return True
    except Exception as e:
        print(f"[VIP-Bio] Gagal proses masuk-VIP uid={user_id} chat={chat_id}: {e}")
        return False


async def _unmute_everywhere(client, chat_id: int, user_id: int) -> None:
    """
    Buka mute/restrict yang sedang aktif untuk user ini di grup ini,
    di KEDUA sisi (bot utama + userbot VC), best-effort.

    Keputusan desain: status VIP baru = state netral. Tidak ada "lanjutan"
    dari hukuman sebelumnya — counter & level mute lokal direset ke 0,
    bukan cuma di-unmute saja (supaya tidak langsung kena mute lagi pada
    pelanggaran pertama setelah VIP berakhir nanti).
    """
    # ── Sisi bot utama: buka restrict pesan teks + reset counter ───────────
    try:
        from core.moderation_queue import queue_unmute
        await reset_local_mute(chat_id, user_id)
        await queue_unmute(chat_id, user_id)
    except Exception as e:
        print(f"[VIP-Bio] Gagal unmute (bot utama) uid={user_id} chat={chat_id}: {e}")

    # ── Sisi userbot VC: antri scan agar mic di-unmute jika sedang muted ───
    try:
        from video_call import _enqueue_vc_scan, _member_cache
        _member_cache.pop((chat_id, user_id), None)
        _enqueue_vc_scan(chat_id)
    except Exception as e:
        print(f"[VIP-Bio] Gagal antri unmute VC uid={user_id} chat={chat_id}: {e}")


async def _exit_vip_bio(chat_id: int, user_id: int) -> None:
    """Hapus status VIP-bio user ini (dipanggil hanya oleh checker loop)."""
    try:
        result = await free_col.delete_one({
            "user_id": user_id, "chat_id": chat_id, "source": "bio_vip",
        })
        if result.deleted_count:
            print(f"[VIP-Bio] uid={user_id} chat={chat_id} → KELUAR VIP (teks hilang dari bio).")
            # ── Title VIP: hapus tag yang terpasang (jika ada) ──────────────
            await clear_vip_title(chat_id, user_id)
            try:
                from video_call import invalidate_vip_cache
                invalidate_vip_cache(chat_id, user_id)
            except Exception:
                pass
    except Exception as e:
        print(f"[VIP-Bio] Gagal proses keluar-VIP uid={user_id} chat={chat_id}: {e}")


_checker_running = False


async def vip_bio_checker_loop() -> None:
    """
    Loop background — cek ulang tiap 10 menit, per grup yang punya
    bio_check=True dan bio_vip_text terisi, SEMUA user yang VIP-nya
    berasal dari teks bio (source="bio_vip"). Jika teks sudah tidak ada
    lagi di bio user → keluarkan dari VIP, tanpa notifikasi apapun.

    VIP manual (/vip, tanpa source="bio_vip") tidak pernah disentuh.

    Didaftarkan sebagai background task tunggal (lihat antigcast.py),
    mengikuti pola newscore_checker_loop / moderation_worker_loop.
    """
    global _checker_running
    if _checker_running:
        return
    _checker_running = True
    print("[VIP-Bio] Checker loop dimulai (interval 10 menit).")

    from monitor_bot_reference import force_check_user

    while True:
        try:
            await asyncio.sleep(600)   # 10 menit

            cfgs = await config_db.find({"bio_check": True}).to_list(length=500)
            for cfg_doc in cfgs:
                chat_id = cfg_doc.get("chat_id")
                if not chat_id:
                    continue
                try:
                    cfg = await get_config(chat_id)
                except Exception:
                    continue

                vip_text = (cfg.get("bio_vip_text") or "").strip()
                if not vip_text:
                    continue   # teks VIP belum diatur → tidak ada yang perlu dicek

                try:
                    vip_docs = await free_col.find(
                        {"chat_id": chat_id, "source": "bio_vip"}
                    ).to_list(length=500)
                except Exception as e:
                    print(f"[VIP-Bio] Gagal query VIP grup={chat_id}: {e}")
                    continue

                for doc in vip_docs:
                    user_id = doc.get("user_id")
                    if not user_id:
                        continue
                    try:
                        fresh_result = await force_check_user(chat_id, user_id)
                    except Exception as e:
                        print(f"[VIP-Bio] force_check_user gagal uid={user_id} chat={chat_id}: {e}")
                        continue

                    if fresh_result is None:
                        # Bot pemantau tidak aktif / gagal fetch fresh — data
                        # bio_profiles yang ada bisa jadi basi/sudah TTL-expired.
                        # FAIL-SAFE: jangan keluarkan dari VIP berdasarkan data
                        # yang tidak fresh. Coba lagi di siklus 10 menit berikutnya.
                        print(
                            f"[VIP-Bio] uid={user_id} chat={chat_id}: bio pemantau "
                            "tidak tersedia (fresh-check gagal) — skip, jangan keluarkan VIP."
                        )
                        await asyncio.sleep(1.0)
                        continue

                    try:
                        bio_doc = await bio_col.find_one(
                            {"chat_id": chat_id, "user_id": user_id}
                        )
                    except Exception as e:
                        print(f"[VIP-Bio] Gagal baca bio_profiles uid={user_id} chat={chat_id}: {e}")
                        continue

                    bio_text = (bio_doc.get("bio", "") if bio_doc else "") or ""
                    still_vip = vip_text.lower() in bio_text.lower()

                    if not still_vip:
                        await _exit_vip_bio(chat_id, user_id)

                    # Jeda kecil antar user agar tidak membebani bot pemantau
                    await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[VIP-Bio] Checker loop error: {e}")
