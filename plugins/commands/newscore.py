"""
plugins/commands/newscore.py
────────────────────────────
Sistem Skor Keaktifan & Admin Otomatis (NewsCore).

Fitur:
  • Track setiap pesan member (non-admin) → tambah skor di MongoDB
  • Background worker → cek waktu reset, angkat admin otomatis
  • /ns_score  — lihat leaderboard grup (admin only)
  • /ns_reset  — paksa reset sekarang (owner only, dev/test)
"""

import asyncio
from datetime import datetime
from html import escape as _html_escape

from pyrogram import Client, filters
from pyrogram.types import Message, ChatPrivileges, ChatMemberUpdated
from pyrogram.enums import ParseMode, ChatMemberStatus
from pyrogram.errors import FloodWait

from database import (
    ns_get_config, ns_update, ns_calc_next_reset,
    ns_track_message, ns_get_leaderboard, ns_reset_scores,
    ns_get_current_admins, ns_set_current_admins,
    ns_get_active_user_count, ns_flush_score_buffer,
    ns_remove_score, invalidate_ns_admins_cache,
    ns_get_titled_members, ns_set_titled_members,
    HARI_MAP_NS, is_admin, TZ_WIB, delete_queue,
)
from plugins.ui.handlers_fsm import _truncate_to_utf16_limit
from core.member_tag import set_chat_member_tag

import os
_OWNER_ID = int(os.environ.get("OWNER_ID", 0))


async def _revoke_vip_on_ns_promote(chat_id: int, user_id: int) -> None:
    """
    Hapus status VIP (manual maupun bio_vip) dari member yang BARU SAJA
    diangkat jadi admin NewsCore.

    KENAPA PERLU: kalau member sudah VIP duluan (lewat /vip atau teks bio)
    lalu kemudian terpilih jadi admin NewsCore lewat skor leaderboard, status
    VIP lamanya akan tetap nyangkut kalau tidak dibersihkan di sini — padahal
    admin NewsCore wajib bisa kena tindak "Bio Admin Wajib", dan status VIP
    membuatnya bebas dari semua filter lain juga (efek yang tidak diinginkan
    untuk pemegang jabatan admin). Pencegahan di sisi MASUK VIP (/vip, panel,
    teks bio) sudah ada — fungsi ini menutup arah sebaliknya: member yang
    SUDAH VIP DULU, baru kemudian jadi admin NewsCore.
    """
    try:
        from database import db
        free_col = db["free_per_group"]
        result = await free_col.delete_one({"chat_id": chat_id, "user_id": user_id})
        if result.deleted_count:
            print(f"[NewsCore] uid={user_id} chat={chat_id} → VIP lama dicabut (sekarang admin NewsCore)")
            try:
                from video_call import invalidate_vip_cache
                invalidate_vip_cache(chat_id, user_id)
            except Exception:
                pass
            # Title VIP: hapus tag VIP yang terpasang (jika ada) — user ini
            # sekarang admin NewsCore, bukan VIP lagi.
            try:
                from core.vip_bio_guard import clear_vip_title
                await clear_vip_title(chat_id, user_id)
            except Exception:
                pass
    except Exception as e:
        print(f"[NewsCore] gagal cabut VIP uid={user_id} chat={chat_id}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
#  TRACK PESAN MEMBER (non-admin only)
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_message((filters.group | filters.forum) & ~filters.service & ~filters.bot, group=15)
async def ns_track(client, message: Message):
    """
    Hitung skor hanya jika:
    - Pengirim bukan bot
    - Pengirim bukan admin/owner grup, KECUALI admin yang diangkat oleh
      bot ini melalui NewsCore periode sebelumnya (NS admin aktif)
    - Pesan bukan command
    - User TIDAK sedang terindikasi ubot (lihat core/ubot_detect.py)

    CATATAN PERFORMA: tidak ada lagi jeda/double-check is_message_handled
    di sini (versi lama menunggu 0.35 detik supaya antispam/bio/cas sempat
    mark_message_handled lebih dulu). Sekarang murni real-time mengikuti
    event pesan asli — lebih ringan karena tidak menengok status pesan yang
    sama dua kali. Sebagai gantinya, akun yang sudah positif terindikasi
    ubot (data ini TETAP terkumpul walau toggle ubot_detect mati — lihat
    docstring core/ubot_detect.py) dikecualikan total dari skor NewsCore,
    supaya hanya user asli yang bisa masuk leaderboard/jadi admin.
    """
    try:
        if not message.from_user or message.from_user.is_bot:
            return
        if message.text and message.text.startswith("/"):
            return

        chat_id = message.chat.id
        user_id = message.from_user.id

        cfg = await ns_get_config(chat_id)
        if not cfg.get("enabled"):
            return

        # Cek apakah user adalah admin di grup
        if await is_admin(client, chat_id, user_id):
            # Izinkan hanya jika dia adalah NS admin (diangkat bot via NewsCore)
            # Admin lain (manual/owner) tetap di-skip
            ns_admins = await ns_get_current_admins(chat_id)
            ns_admin_ids = {a["user_id"] for a in ns_admins}
            if user_id not in ns_admin_ids:
                return

        # Cek status deteksi ubot — data ini terus terkumpul terlepas toggle
        # ubot_detect ON/OFF (lihat core/ubot_detect.py), jadi bisa langsung
        # dipakai sebagai gate di sini walau fitur deteksinya sendiri mati.
        # User yang POSITIF terindikasi ubot dikecualikan total dari skor:
        # - skornya dihapus (kalau ada sisa dari sebelum terdeteksi)
        # - pesan ini TIDAK dihitung
        # Begitu dia keluar dari status ubot, hanya pesan SETELAHNYA yang
        # mulai dihitung lagi (mulai dari 0, karena skor lama sudah dihapus).
        # Kalau dia masuk status ubot lagi nanti, skornya dihapus ulang.
        from core.ubot_detect import get_user_sentence_summary
        summary = await get_user_sentence_summary(chat_id, user_id)
        if summary.get("terindikasi_ubot"):
            await ns_remove_score(chat_id, user_id)
            return

        await ns_track_message(
            chat_id=chat_id,
            user_id=user_id,
            user_name=message.from_user.first_name or "User",
        )
    except Exception as e:
        print(f"[NewsCore] track handler error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
#  LEADERBOARD COMMAND  /ns_score
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("ns_score") & (filters.group | filters.forum), group=20)
async def cmd_ns_score(client, message: Message):
    try:
        chat_id = message.chat.id
        uid     = message.from_user.id if message.from_user else 0
        if not await is_admin(client, chat_id, uid):
            return

        cfg = await ns_get_config(chat_id)
        if not cfg.get("enabled"):
            rep = await message.reply_text(
                "⚠️ <b>NewsCore</b> belum diaktifkan di grup ini.\n"
                "Aktifkan via <b>⚙️ Kelola Grup → 🏆 NewsCore</b>.",
                parse_mode=ParseMode.HTML,
            )
            asyncio.create_task(_auto_del([message, rep], 10))
            return

        # Flush buffer dulu agar skor yang belum di-DB ikut tampil
        await ns_flush_score_buffer()
        top = await ns_get_leaderboard(chat_id, 10)
        total_aktif = await ns_get_active_user_count(chat_id)
        if not top:
            rep = await message.reply_text(
                "📭 Belum ada data keaktifan periode ini.",
                parse_mode=ParseMode.HTML,
            )
            asyncio.create_task(_auto_del([message, rep], 10))
            return

        lines = "".join(
            f"{i}. <b>{_html_escape(str(m['user_name']))}</b> — <code>{m['score']}</code> poin\n"
            for i, m in enumerate(top, 1)
        )

        next_r = cfg.get("next_reset")
        next_str = ""
        if next_r:
            try:
                next_str = f"\n📅 Reset berikutnya: <code>{datetime.fromisoformat(next_r).strftime('%d %b %Y %H:%M')}</code> WIB"
            except Exception:
                pass

        rep = await message.reply_text(
            f"🏆 <b>PAPAN SKOR KEAKTIFAN</b>\n"
            f"<code>Grup: {chat_id}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👥 <b>Total user aktif periode ini:</b> <code>{total_aktif}</code>\n\n"
            f"{lines}"
            f"{next_str}",
            parse_mode=ParseMode.HTML,
        )
        asyncio.create_task(_auto_del([message, rep], 30))
    except Exception as e:
        print(f"[NewsCore] /ns_score error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
#  FORCE RESET COMMAND  /ns_reset  (owner only)
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_message(filters.command("ns_reset") & (filters.group | filters.forum), group=20)
async def cmd_ns_reset(client, message: Message):
    try:
        uid = message.from_user.id if message.from_user else 0
        if uid != _OWNER_ID:
            return
        await message.reply_text("⏳ Memulai simulasi reset NewsCore…")
        await ns_do_reset(client, message.chat.id)
    except Exception as e:
        print(f"[NewsCore] /ns_reset error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
#  CORE RESET JOB
# ─────────────────────────────────────────────────────────────────────────────

# Jeda (detik) antar setChatMemberTag agar tidak FloodWait.
# Juga dipakai antar promote_chat_member di ns_do_reset.
_NS_ACTION_DELAY = float(os.environ.get("NS_ACTION_DELAY", 0.5))


async def _apply_auto_title_member(
    client, chat_id: int, cfg: dict, admin_ids: set,
    *, base_delay: float = 0.0
) -> str:
    """
    Pasang tag otomatis ke member NON-admin berdasar rank leaderboard typing
    NewsCore, sesuai kelompok 5-rank per nama yang diisi owner (maks 10 nama
    -> cover rank 1-50). Dipanggil dari ns_do_reset(), terpisah dari logika
    pengangkatan admin di atas.

    PEMBERSIHAN TITEL LAMA (penting):
    Member yang dititel periode SEBELUMNYA tapi TIDAK lagi masuk daftar
    kandidat titel periode BARU akan di-hapus tag-nya (setChatMemberTag
    dengan tag=""). Daftar member bertitel disimpan di newscore_titled_db
    (ns_get_titled_members / ns_set_titled_members) justru supaya
    perbandingan lama-vs-baru ini bisa dilakukan tiap reset, walau owner
    mematikan/menyalakan fitur ini di antara periode.

    admin_ids: kumpulan user_id yang BARU diangkat admin periode ini.
               Mereka di-exclude dari pemberian tag member karena:
               1. Mereka adalah admin (setChatMemberTag hanya untuk non-admin).
               2. Mereka sudah dapat custom_title via set_administrator_title.
               Juga termasuk NS admin LAMA yang masih admin (dari ns_get_current_admins)
               agar tidak salah pasang tag ke admin yang tidak tercabut.

               Member VIP (manual /vip ATAU bio_vip) JUGA dikecualikan dari
               Auto Title Member — diambil di dalam fungsi ini sendiri lewat
               get_vip_user_ids(), sama seperti admin di atas. Tujuannya:
               member VIP tidak "mencuri" slot 5-per-tier dari member biasa
               yang sebenarnya berhak (VIP biasanya sudah bebas dari segala
               filter, jadi tidak perlu lagi dapat tag titel typing juga),
               dan supaya jumlah member bertitel NewsCore tetap LENGKAP
               (5 per tier) walau ada member VIP yang typing aktif di
               leaderboard.

    base_delay: offset delay awal (detik) untuk stagger antar grup saat reset
               berjalan bersamaan — cegah semua grup hit API di waktu sama.

    Returns ringkasan singkat (string) untuk disisipkan ke pengumuman reset,
    atau "" jika fitur tidak aktif / tidak ada nama diisi / tidak ada member
    yang memenuhi syarat DAN tidak ada titel lama yang perlu dibersihkan.
    """
    auto_title_active = cfg.get("auto_title_enabled", False)
    names = [n for n in cfg.get("auto_title_names", []) if n and n.strip()]

    # Member VIP dikecualikan dari Auto Title Member — sama seperti admin.
    # Lihat penjelasan lengkap di docstring param admin_ids di atas.
    from database import get_vip_user_ids
    vip_ids = await get_vip_user_ids(chat_id)
    excluded_ids = admin_ids | vip_ids

    # Titel lama dari periode sebelumnya — dibaca terlepas dari status aktif
    # sekarang, supaya kalau owner baru MEMATIKAN fitur ini, titel yang sudah
    # terpasang tetap dibersihkan pada reset berikutnya (bukan dibiarkan
    # nyangkut selamanya).
    old_titled = await ns_get_titled_members(chat_id)
    old_titled_by_uid = {m["user_id"]: m for m in old_titled}

    if not auto_title_active or not names:
        # Fitur OFF (atau belum diisi nama) → tidak pasang titel baru,
        # tapi tetap bersihkan SEMUA titel lama yang masih nyangkut.
        if not old_titled:
            return ""
        cleared = 0
        for idx, m in enumerate(old_titled):
            uid = m["user_id"]
            if idx > 0:
                await asyncio.sleep(_NS_ACTION_DELAY)
            try:
                success, _ = await set_chat_member_tag(chat_id, uid, "")
                if success:
                    cleared += 1
            except Exception as e:
                print(f"[NewsCore][AutoTitle] gagal hapus tag lama uid={uid}: {e}")
        await ns_set_titled_members(chat_id, [])
        if cleared:
            return f"\n\n🏷️ <b>Auto Title Member:</b> nonaktif — <code>{cleared}</code> titel lama dibersihkan."
        return ""

    # Butuh leaderboard sampai cover seluruh kelompok nama yang diisi
    # (maks 10 nama x 5 rank = 50), supaya rank terakhir tetap dapat tag
    # walau owner mengisi semua 10 slot. Pool diperbesar dengan jumlah
    # excluded_ids (admin + VIP) supaya pengecualian VIP TIDAK mengurangi
    # kuota 5-per-tier — leaderboard tetap digali lebih dalam sampai cukup
    # member non-admin & non-VIP untuk mengisi semua slot.
    pool_size  = len(names) * 5
    full_board = await ns_get_leaderboard(chat_id, pool_size + len(excluded_ids) + 10)

    # Saring member yang baru jadi admin periode ini, admin NS lama, DAN
    # member VIP aktif (excluded_ids = admin_ids | vip_ids). Admin tidak
    # boleh dapat tag member (Telegram API akan menolak); VIP dikecualikan
    # by design agar kuota title member tetap penuh untuk non-VIP.
    candidates = [w for w in full_board if w["user_id"] not in excluded_ids][:pool_size]

    if base_delay > 0:
        await asyncio.sleep(base_delay)

    # ── Bangun daftar titel BARU (rank → nama tag) ────────────────────────
    new_titled: dict[int, dict] = {}
    ok_count, fail_count = 0, 0
    fail_samples = []

    for idx, w in enumerate(candidates):
        group_idx = idx // 5  # 0 = rank 1-5, 1 = rank 6-10, dst
        if group_idx >= len(names):
            break
        tag = _truncate_to_utf16_limit(names[group_idx], 16)
        uid = w["user_id"]

        # Jeda antar member untuk menghindari FloodWait setChatMemberTag
        if idx > 0:
            await asyncio.sleep(_NS_ACTION_DELAY)

        success, reason = await set_chat_member_tag(chat_id, uid, tag)
        if success:
            ok_count += 1
            new_titled[uid] = {
                "chat_id": chat_id, "user_id": uid,
                "user_name": w.get("user_name", str(uid)), "tag": tag,
            }
        else:
            fail_count += 1
            if len(fail_samples) < 3:
                fail_samples.append(f"{w.get('user_name', uid)}: {reason}")
            print(f"[NewsCore][AutoTitle] gagal uid={uid} tag={tag!r}: {reason}")
            # Tetap dianggap "masih bertitel sesuai data lama" jika gagal
            # diupdate — supaya tidak salah dibersihkan di reset berikutnya
            # hanya karena satu kegagalan API sesaat. Jika user ini memang
            # ada di old_titled, biarkan dia tetap tercatat dengan tag baru
            # yang seharusnya (agar percobaan berikutnya bisa retry natural
            # lewat reset selanjutnya); jika tidak ada di old_titled, lewati.
            if uid in old_titled_by_uid:
                new_titled[uid] = {
                    "chat_id": chat_id, "user_id": uid,
                    "user_name": w.get("user_name", str(uid)), "tag": tag,
                }

    # ── Hapus tag dari member LAMA yang TIDAK lagi masuk daftar baru ──────
    stale_uids = [uid for uid in old_titled_by_uid if uid not in new_titled]
    cleared_count = 0
    for idx, uid in enumerate(stale_uids):
        if idx > 0 or new_titled:
            await asyncio.sleep(_NS_ACTION_DELAY)
        try:
            success, reason = await set_chat_member_tag(chat_id, uid, "")
            if success:
                cleared_count += 1
            else:
                print(f"[NewsCore][AutoTitle] gagal hapus tag lama uid={uid}: {reason}")
        except Exception as e:
            print(f"[NewsCore][AutoTitle] gagal hapus tag lama uid={uid}: {e}")

    # ── Simpan daftar bertitel terbaru ke DB ──────────────────────────────
    await ns_set_titled_members(chat_id, list(new_titled.values()))

    if ok_count == 0 and fail_count == 0 and cleared_count == 0:
        return ""

    summary = f"\n\n🏷️ <b>Auto Title Member:</b> <code>{ok_count}</code> member ditandai otomatis."
    if cleared_count:
        summary += f" <code>{cleared_count}</code> titel lama dibersihkan (tidak masuk daftar baru)."
    if fail_count:
        summary += (
            f"\n⚠️ <code>{fail_count}</code> gagal — kemungkinan bot belum "
            f"punya hak <code>can_manage_tags</code>."
        )
    return summary


# Semaphore global: batasi berapa grup yang diproses reset bersamaan.
# Default 2 → maks 2 grup reset paralel; sisanya antri.
# Cegah semua grup yg jadwal resetnya sama persis langsung memborbardir API.
_ns_reset_semaphore = asyncio.Semaphore(int(os.environ.get("NS_RESET_CONCURRENCY", 2)))

# Stagger offset per grup (detik) — diset saat checker_loop menemukan
# beberapa grup yang waktu resetnya sudah lewat di iterasi yang sama.
# {chat_id: offset_detik}
_ns_reset_stagger: dict[int, float] = {}


async def ns_do_reset(client, chat_id: int):
    """
    Angkat admin berdasarkan skor tertinggi, lalu reset semua skor.

    RATE-LIMIT SAFE:
    - Semaphore _ns_reset_semaphore membatasi reset paralel antar grup.
    - _NS_ACTION_DELAY jeda antar promote_chat_member / setChatMemberTag.
    - Auto Title Member exclude NS admin baru DAN admin NS lama (semua admin
      aktif saat ini) agar tidak mencoba pasang tag ke user yang admin.
    - base_delay (stagger) dipakai untuk offset auto title antar grup.
    """
    stagger = _ns_reset_stagger.pop(chat_id, 0.0)
    if stagger > 0:
        await asyncio.sleep(stagger)

    async with _ns_reset_semaphore:
        await _ns_do_reset_impl(client, chat_id)


async def _ns_do_reset_impl(client, chat_id: int):
    """Implementasi inti reset — hanya dipanggil via ns_do_reset (sudah ada semaphore)."""
    try:
        # Ambil config terbaru dari DB (bukan cache lama)
        cfg         = await ns_get_config(chat_id)
        max_admins  = cfg.get("max_admins", 1)
        p           = cfg.get("privileges", {})
        admin_title = (cfg.get("admin_title") or "").strip()

        # Flush buffer sebelum ambil leaderboard → skor terbaru masuk DB
        await ns_flush_score_buffer()

        # ── MODE TITLE-ONLY (max_admins == 0) ────────────────────────────────
        # Tidak ada admin yang diangkat atau dicopot.
        # Hanya jalankan Auto Title Member (jika aktif) lalu reset skor.
        if max_admins == 0:
            # Ambil daftar NS admin lama — mereka tetap admin, tidak disentuh
            old_admins    = await ns_get_current_admins(chat_id)
            old_admin_ids = {a["user_id"] for a in old_admins}

            ann = (
                "📢 <b>RESET NEWSCORE — MODE TITLE MEMBER</b> 📢\n\n"
                "ℹ️ Kuota admin diset ke <code>0</code> — tidak ada admin diangkat periode ini.\n"
                "Hanya penilaian title member yang berjalan.\n"
            )

            # Auto Title Member tetap berjalan penuh
            # Exclude NS admin lama agar tidak dicoba di-tag (Telegram tolak tag ke admin)
            auto_title_summary = await _apply_auto_title_member(
                client, chat_id, cfg, old_admin_ids, base_delay=0.0
            )
            ann += auto_title_summary

            # Hitung next_reset
            cfg_fresh = await ns_get_config(chat_id)
            new_next  = ns_calc_next_reset(cfg_fresh)
            await ns_update(chat_id, {"next_reset": new_next})

            ann += (
                f"\n\n🔄 <i>Poin direset ke 0!</i>\n"
                f"📅 Reset berikutnya: <code>{datetime.fromisoformat(new_next).strftime('%d %b %Y %H:%M')}</code> WIB"
            )

            try:
                await client.send_message(chat_id=chat_id, text=ann, parse_mode=ParseMode.HTML)
            except Exception as e:
                print(f"[NewsCore] send announcement error: {e}")

            await ns_reset_scores(chat_id)
            return

        # ── MODE NORMAL (max_admins > 0) ──────────────────────────────────────
        top = await ns_get_leaderboard(chat_id, max_admins)

        # Ambil daftar admin NS lama (sebelum periode ini)
        old_admins     = await ns_get_current_admins(chat_id)
        old_admin_ids  = {a["user_id"] for a in old_admins}
        new_ids        = {m["user_id"] for m in top}

        # Copot admin lama yang tidak masuk top baru (+ jeda antar copot)
        for i, old in enumerate(old_admins):
            if old["user_id"] not in new_ids:
                if i > 0:
                    await asyncio.sleep(_NS_ACTION_DELAY)
                try:
                    await client.promote_chat_member(
                        chat_id=chat_id, user_id=old["user_id"],
                        privileges=ChatPrivileges(can_manage_chat=False),
                    )
                except FloodWait as fw:
                    await asyncio.sleep(fw.value + 1)
                    try:
                        await client.promote_chat_member(
                            chat_id=chat_id, user_id=old["user_id"],
                            privileges=ChatPrivileges(can_manage_chat=False),
                        )
                    except Exception:
                        pass
                except Exception:
                    pass

        ann = "📢 <b>PERGANTIAN ADMIN NEWSCORE PERIODE BARU!</b> 📢\n\n"
        new_admin_docs = []

        if top:
            ann += f"🏆 <b>Top {len(top)} member teraktif:</b>\n\n"
            for idx, w in enumerate(top, 1):
                uid   = w["user_id"]
                uname = w["user_name"]

                # Jeda antar promosi admin (idx > 0 berarti bukan yang pertama)
                if idx > 1:
                    await asyncio.sleep(_NS_ACTION_DELAY)

                # Retry sekali jika kena FloodWait
                for _attempt in range(2):
                    try:
                        await client.promote_chat_member(
                            chat_id=chat_id, user_id=uid,
                            privileges=ChatPrivileges(
                                can_manage_chat=True,
                                can_delete_messages=p.get("can_delete_messages", True),
                                can_restrict_members=p.get("can_restrict_members", True),
                                can_invite_users=p.get("can_invite_users", True),
                                can_pin_messages=p.get("can_pin_messages", True),
                                can_manage_video_chats=p.get("can_manage_video_chats", False),
                            ),
                        )
                        title_ok = False
                        title    = admin_title if admin_title else f"Top Member {idx} 👑"
                        title    = _truncate_to_utf16_limit(title, 16)

                        # Jeda singkat sebelum set_administrator_title
                        # (Telegram butuh waktu catat status admin baru)
                        await asyncio.sleep(0.8)

                        for _title_attempt in range(3):
                            try:
                                await client.set_administrator_title(
                                    chat_id, uid, title
                                )
                                title_ok = True
                                break
                            except FloodWait as fw_title:
                                await asyncio.sleep(fw_title.value + 1)
                                continue
                            except Exception as e_title:
                                print(f"[NewsCore] set_custom_title gagal uid={uid} attempt={_title_attempt+1}: {e_title}")
                                await asyncio.sleep(1.5)
                                continue
                        if not title_ok:
                            print(f"[NewsCore] set_custom_title MENYERAH uid={uid} title={title!r}")
                        new_admin_docs.append({"chat_id": chat_id, "user_id": uid, "user_name": uname})
                        # Member ini sekarang admin NewsCore — pastikan status
                        # VIP lama (kalau ada) tidak nyangkut, supaya bio guard
                        # NewsCore tetap bisa menindaknya secara normal.
                        await _revoke_vip_on_ns_promote(chat_id, uid)
                        title_note = "" if title_ok else " (⚠️ titel gagal dipasang)"
                        ann += f"{idx}. <a href='tg://user?id={uid}'>{_html_escape(uname)}</a> — <code>{w['score']}</code> poin{title_note}\n"
                        break
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value + 1)
                        continue
                    except Exception as e:
                        print(f"[NewsCore] promote error uid={uid}: {e}")
                        ann += f"{idx}. <b>{_html_escape(uname)}</b> (⚠️ gagal dipromosikan)\n"
                        break
                else:
                    print(f"[NewsCore] promote uid={uid} gagal setelah retry FloodWait")
                    ann += f"{idx}. <b>{_html_escape(uname)}</b> (⚠️ gagal dipromosikan — FloodWait)\n"
        else:
            ann += "Tidak ada aktivitas periode ini. Posisi admin tetap. 🏝️"

        # Syarat bio admin wajib
        if top:
            bio_admin_text     = (cfg.get("bio_admin_text") or "").strip()
            bio_admin_required = cfg.get("bio_admin_required", True)
            if bio_admin_required and bio_admin_text:
                ann += (
                    f"\n\n📝 <b>Wajib!</b> Admin di atas harus mencantumkan "
                    f"teks berikut di bio Telegram:\n"
                    f"<code>{_html_escape(bio_admin_text)}</code>\n"
                    f"<i>Bio tidak sesuai → otomatis di-unadmin.</i>"
                )
            elif bio_admin_required and not bio_admin_text:
                ann += (
                    f"\n\n⚠️ <b>Perhatian:</b> Syarat bio admin wajib aktif "
                    f"tapi teksnya belum diatur owner — admin di atas berisiko "
                    f"di-unadmin otomatis sampai diatur."
                )

        # Auto Title Member: exclude admin NS baru (new_ids) DAN admin NS lama
        # (old_admin_ids) yang belum dicabut — total admin aktif tidak boleh
        # dapat tag member (Telegram tolak setChatMemberTag pada admin).
        all_excluded = new_ids | old_admin_ids
        auto_title_summary = await _apply_auto_title_member(
            client, chat_id, cfg, all_excluded, base_delay=0.0
        )
        ann += auto_title_summary

        await ns_set_current_admins(chat_id, new_admin_docs)

        # Hitung next_reset dari config terbaru
        cfg_fresh = await ns_get_config(chat_id)
        new_next  = ns_calc_next_reset(cfg_fresh)
        await ns_update(chat_id, {"next_reset": new_next})

        ann += (
            f"\n\n🔄 <i>Poin direset ke 0!</i>\n"
            f"📅 Reset berikutnya: <code>{datetime.fromisoformat(new_next).strftime('%d %b %Y %H:%M')}</code> WIB"
        )

        try:
            await client.send_message(chat_id=chat_id, text=ann, parse_mode=ParseMode.HTML)
        except Exception as e:
            print(f"[NewsCore] send announcement error: {e}")

        # Reset skor SETELAH pengumuman dikirim (ns_reset_scores flush buffer lagi)
        await ns_reset_scores(chat_id)

    except Exception as e:
        print(f"[NewsCore] ns_do_reset error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
#  BACKGROUND TIME-CHECKER LOOP
# ─────────────────────────────────────────────────────────────────────────────

_checker_running = False


async def newscore_checker_loop(client):
    global _checker_running
    if _checker_running:
        return
    _checker_running = True
    print("[NewsCore] Time-checker loop started.")
    while True:
        try:
            from database import newscore_cfg_db
            all_cfgs = await newscore_cfg_db.find({"enabled": True}).to_list(length=200)
            now = datetime.now(TZ_WIB)

            # Kumpulkan semua grup yang waktunya reset di iterasi ini
            due_groups = []
            for cfg in all_cfgs:
                cid      = cfg.get("chat_id")
                next_str = cfg.get("next_reset")
                if cid and next_str:
                    try:
                        target = datetime.fromisoformat(next_str)
                        if target.tzinfo is None:
                            target = target.replace(tzinfo=TZ_WIB)
                        if now >= target:
                            due_groups.append(cid)
                    except Exception as e:
                        print(f"[NewsCore] checker parse error cid={cid}: {e}")

            if due_groups:
                # Stagger: grup pertama langsung, berikutnya dapat offset
                # _NS_ACTION_DELAY * 10 per grup → cegah flood serentak.
                stagger_unit = _NS_ACTION_DELAY * 10
                for i, cid in enumerate(due_groups):
                    offset = i * stagger_unit
                    if offset > 0:
                        _ns_reset_stagger[cid] = offset
                    print(f"[NewsCore] Reset terjadwal grup {cid} (stagger {offset:.1f}s)")
                    # Fire-and-forget: reset berjalan paralel tapi dibatasi
                    # semaphore _ns_reset_semaphore (maks NS_RESET_CONCURRENCY grup)
                    asyncio.create_task(ns_do_reset(client, cid))

        except Exception as e:
            print(f"[NewsCore] checker error: {e}")
        await asyncio.sleep(30)


# ─────────────────────────────────────────────────────────────────────────────
#  SWEEP BERKALA: INSPEKSI BIO ADMIN NEWSCORE (jam 03:00 WIB)
# ─────────────────────────────────────────────────────────────────────────────
#
# TUJUAN:
#   _check_ns_admin_bio (plugins/filters/bio.py) hanya tertrigger reaktif —
#   saat admin NewsCore mengirim pesan atau typing. Admin yang sudah
#   diangkat lalu DIAM (tidak chat lagi) atau yang mengubah/menghapus bio
#   setelah diangkat tidak akan pernah dicek ulang sampai dia kembali aktif.
#   Sweep ini menutup celah itu dengan memaksa cek bio fresh (via bot
#   pembantu/monitor) untuk SEMUA admin NewsCore aktif, di SEMUA grup yang
#   NewsCore-nya aktif & punya admin NewsCore — bergiliran, 1x per hari.
#
# RATE-LIMIT SAFE:
#   - Hanya grup yang punya admin NewsCore aktif yang diproses (skip grup
#     kosong → tidak buang waktu/API).
#   - Bergiliran (1 grup diproses penuh dulu) dengan jeda antar grup
#     maupun antar user — tidak burst ke Telegram / bot pembantu.
#   - force_check_user() tetap lewat _bio_worker per-grup (rate-limited,
#     FloodWait-aware) — sweep ini tidak bypass mekanisme itu.
#   - enforce_admin_bio() sendiri sudah punya cooldown internal, aman
#     dipanggil berulang.

_NS_BIO_SWEEP_HOUR   = int(os.environ.get("NS_BIO_SWEEP_HOUR", 3))
_NS_BIO_SWEEP_MINUTE = int(os.environ.get("NS_BIO_SWEEP_MINUTE", 0))
_ns_bio_sweep_last_date = None  # tanggal (date) terakhir sweep selesai dijalankan
_ns_bio_sweep_running = False


async def _ns_bio_sweep_one_group(client, chat_id: int) -> None:
    """
    Inspeksi bio semua admin NewsCore aktif di satu grup.

    Untuk setiap admin: paksa fetch bio fresh via bot pembantu
    (force_check_user, yang otomatis menulis admin_bio_ok ke bio_profiles
    lewat check_admin_bio_text), lalu baca hasilnya dan eksekusi
    enforce_admin_bio jika tidak patuh.
    """
    try:
        ns_cfg = await ns_get_config(chat_id)
        if not ns_cfg.get("enabled"):
            return

        ns_admins = await ns_get_current_admins(chat_id)
        if not ns_admins:
            return

        from monitor_bot_reference import force_check_user, query_admin_bio_ok
        from core.ns_bio_guard import enforce_admin_bio

        for idx, admin_doc in enumerate(ns_admins):
            uid = admin_doc.get("user_id")
            if not uid:
                continue
            if idx > 0:
                await asyncio.sleep(_NS_ACTION_DELAY)
            try:
                # force_check_user → fetch bio fresh via bot pembantu grup ini,
                # otomatis menghitung & menyimpan admin_bio_ok terbaru.
                await force_check_user(chat_id, uid)
                admin_bio_ok = await query_admin_bio_ok(chat_id, uid)
                if admin_bio_ok is False:
                    await enforce_admin_bio(client, chat_id, uid, admin_bio_ok)
            except Exception as e:
                print(f"[NewsCore][BioSweep] gagal cek uid={uid} chat={chat_id}: {e}")
    except Exception as e:
        print(f"[NewsCore][BioSweep] gagal proses chat={chat_id}: {e}")


async def newscore_bio_sweep_loop(client):
    """
    Loop berkala: setiap hari jam 03:00 WIB (default, bisa diubah via env
    NS_BIO_SWEEP_HOUR/NS_BIO_SWEEP_MINUTE), inspeksi bio seluruh admin
    NewsCore di seluruh grup yang fitur NewsCore-nya aktif & punya admin
    NewsCore — bergiliran satu per satu grup.

    Jalankan sekali dari antigcast.py setelah await app.start(), sama
    seperti newscore_checker_loop.
    """
    global _ns_bio_sweep_last_date, _ns_bio_sweep_running
    if _ns_bio_sweep_running:
        return
    _ns_bio_sweep_running = True
    print("[NewsCore][BioSweep] Loop sweep bio admin dimulai.")

    while True:
        try:
            now = datetime.now(TZ_WIB)
            due = (
                now.hour == _NS_BIO_SWEEP_HOUR
                and now.minute >= _NS_BIO_SWEEP_MINUTE
                and _ns_bio_sweep_last_date != now.date()
            )
            if due:
                print("[NewsCore][BioSweep] Mulai sweep bio admin NewsCore harian.")
                from database import newscore_cfg_db
                all_cfgs = await newscore_cfg_db.find({"enabled": True}).to_list(length=500)
                for cfg in all_cfgs:
                    cid = cfg.get("chat_id")
                    if not cid:
                        continue
                    try:
                        await _ns_bio_sweep_one_group(client, cid)
                    except Exception as e:
                        print(f"[NewsCore][BioSweep] error grup {cid}: {e}")
                    # Jeda antar grup — bergiliran, tidak burst semua grup sekaligus
                    await asyncio.sleep(_NS_ACTION_DELAY * 4)
                _ns_bio_sweep_last_date = now.date()
                print("[NewsCore][BioSweep] Sweep bio admin NewsCore harian selesai.")
        except Exception as e:
            print(f"[NewsCore][BioSweep] loop error: {e}")
        await asyncio.sleep(30)


# ─────────────────────────────────────────────────────────────────────────────
#  Helper
# ─────────────────────────────────────────────────────────────────────────────

async def _auto_del(msgs: list, delay: int):
    """
    Hapus pesan setelah `delay` detik via delete_queue (bukan loop direct delete).
    Pesan dikelompokkan per chat_id sehingga worker dapat mengirim
    1 delete_messages(cid, [...]) per chat — aman dari burst API.
    """
    await asyncio.sleep(delay)
    grouped: dict[int, list[int]] = {}
    for m in msgs:
        try:
            grouped.setdefault(m.chat.id, []).append(m.id)
        except Exception:
            pass
    for cid, mids in grouped.items():
        try:
            await delete_queue.put((cid, mids))
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
#  DETEKSI ADMIN PAKSA — hapus dari count NewsCore
# ─────────────────────────────────────────────────────────────────────────────

@Client.on_chat_member_updated(group=16)
async def ns_watch_forced_admin(client: Client, update: ChatMemberUpdated):
    """
    Deteksi member yang di-adminkan PAKSA oleh owner/admin lain (bukan via
    NewsCore). Jika terdeteksi, hapus skor mereka dari newscore_stats agar:
    - Bot tidak mencoba meng-adminkan mereka lagi di periode berikutnya
      (mereka sudah admin, promote_chat_member akan gagal atau konflik hak)
    - Leaderboard tidak memasukkan mereka sebagai kandidat

    Logika deteksi "admin paksa":
      old_status = member biasa (MEMBER / RESTRICTED)
      new_status = ADMINISTRATOR
      user_id TIDAK ADA di daftar NS admin aktif (ns_get_current_admins)

    Jika user sudah ada di daftar NS admin → berarti ini adalah pengangkatan
    yang dilakukan oleh NewsCore sendiri → SKIP, jangan hapus skornya.

    group=16 → jalan setelah ns_track (group=15), tidak ada konflik.
    """
    try:
        if not update.new_chat_member or not update.old_chat_member:
            return

        new_status = update.new_chat_member.status
        old_status = update.old_chat_member.status

        # Hanya peduli: member biasa → admin
        was_admin = old_status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
        now_admin = new_status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)

        if was_admin or not now_admin:
            return  # bukan promosi baru → skip

        user = update.new_chat_member.user
        if not user or user.is_bot:
            return

        chat_id = update.chat.id
        user_id = user.id

        cfg = await ns_get_config(chat_id)
        if not cfg.get("enabled"):
            return

        # Cek apakah ini pengangkatan oleh NewsCore (ada di daftar NS admin)
        ns_admins    = await ns_get_current_admins(chat_id)
        ns_admin_ids = {a["user_id"] for a in ns_admins}

        if user_id in ns_admin_ids:
            # Diangkat oleh NewsCore sendiri → jangan hapus skor
            return

        # Admin paksa dari luar NewsCore → hapus dari count
        await ns_remove_score(chat_id, user_id)
        print(
            f"[NewsCore] uid={user_id} di-adminkan paksa di chat={chat_id} "
            f"(bukan via NewsCore) → skor dihapus dari count"
        )

    except Exception as e:
        print(f"[NewsCore] ns_watch_forced_admin error: {e}")

