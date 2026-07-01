"""
core/antispam_queue.py
───────────────────────────────────────────────────────────────────────────────
Worker antrian deteksi spam untuk antispam.py.

ARSITEKTUR (3 LAPISAN):
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  LAPISAN 1 — Fast-path in-handler (tetap di antispam.py)               │
  │  • is_message_handled() / is_admin() / free_col → return cepat         │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  LAPISAN 2 — detection_queue + _process_detection (file ini)           │
  │                                                                         │
  │  DETEKSI PARALEL + MENTION SEQUENTIAL                                   │
  │  Setiap pesan diproses oleh 4 gate paralel:                             │
  │    Gate A — regex global + lokal   (CPU + cache)                        │
  │    Gate B — link detector          (CPU)                                │
  │    Gate C — anti duplikasi lokal   (MongoDB)                            │
  │    Gate D — anti gcast global      (MongoDB)                            │
  │                                                                         │
  │  Koordinasi via asyncio.Event "found":                                  │
  │    • Gate pertama yang temukan spam → set found, masuk delete_queue     │
  │    • Gate lain cek found sebelum/sesudah await → stop lebih awal        │
  │    • mark_message_handled() mencegah double-action                      │
  │    • asyncio single-threaded → check+set di titik non-await aman        │
  │                                                                         │
  │  Gate E (mention check) berjalan SEQUENTIAL setelah 4 gate paralel     │
  │  selesai — karena menyentuh Telegram API + flood-sensitive.             │
  │  Hanya berjalan jika semua gate paralel miss.                           │
  ├─────────────────────────────────────────────────────────────────────────┤
  │  LAPISAN 3 — Action queue (tidak berubah)                               │
  │  • delete_queue → delete_worker                                         │
  │  • moderation_queue → moderation_worker_loop                            │
  └─────────────────────────────────────────────────────────────────────────┘

PARAMETER TUNING (via .env):
  ANTISPAM_QUEUE_MAXSIZE   default 500  — max item antrian sebelum drop
  ANTISPAM_DETECT_DELAY    default 0.02 — jeda antar pesan (detik)
  ANTISPAM_MENTION_TIMEOUT default 8.0  — timeout Telegram API mention
"""

import asyncio
import os
import time
from typing import TYPE_CHECKING

from pyrogram.errors import FloodWait
from database import set_global_flood_backoff, wait_global_flood_backoff

if TYPE_CHECKING:
    from pyrogram import Client
    from pyrogram.types import Message

# ── Tuning ───────────────────────────────────────────────────────────────────
_MAXSIZE          = int(os.environ.get("ANTISPAM_QUEUE_MAXSIZE",  500))
_DETECT_DELAY     = float(os.environ.get("ANTISPAM_DETECT_DELAY",  0.02))
_MENTION_TIMEOUT  = float(os.environ.get("ANTISPAM_MENTION_TIMEOUT", 8.0))

# ── Queue utama ───────────────────────────────────────────────────────────────
detection_queue: asyncio.Queue = asyncio.Queue(maxsize=_MAXSIZE)

# ── Statistik ringan (debug) ──────────────────────────────────────────────────
_stat_enqueued   = 0
_stat_processed  = 0
_stat_dropped    = 0


def get_detection_queue_stats() -> dict:
    return {
        "enqueued":  _stat_enqueued,
        "processed": _stat_processed,
        "dropped":   _stat_dropped,
        "qsize":     detection_queue.qsize(),
    }


async def enqueue_for_detection(client: "Client", message: "Message") -> bool:
    global _stat_enqueued, _stat_dropped
    try:
        detection_queue.put_nowait((client, message))
        _stat_enqueued += 1
        return True
    except asyncio.QueueFull:
        _stat_dropped += 1
        print(
            f"[antispam_queue] ⚠️  Queue penuh ({_MAXSIZE}) — "
            f"pesan mid={message.id} cid={message.chat.id} di-drop"
        )
        return False


async def antispam_detection_worker(client: "Client") -> None:
    """
    Worker tunggal: ambil 1 pesan dari queue, proses paralel, jeda, ulangi.
    """
    global _stat_processed

    for _ in range(60):
        if getattr(client, "is_connected", False):
            break
        await asyncio.sleep(1.0)

    print("[antispam_queue] ✅ Worker deteksi antispam siap.", flush=True)

    while True:
        try:
            item = await detection_queue.get()
        except asyncio.CancelledError:
            break

        try:
            _client, _message = item
            await _process_detection(_client, _message)
            _stat_processed += 1
        except asyncio.CancelledError:
            detection_queue.task_done()
            break
        except Exception as e:
            print(f"[antispam_queue] ❌ Error proses pesan: {e}")
        finally:
            detection_queue.task_done()

        await asyncio.sleep(_DETECT_DELAY)


# ─────────────────────────────────────────────────────────────────────────────
#  Helper klaim — asyncio single-threaded, check+set di non-await point aman
# ─────────────────────────────────────────────────────────────────────────────

def _try_claim(found: asyncio.Event, cid: int, mid: int,
               is_message_handled, mark_message_handled) -> bool:
    """
    Coba klaim pesan sebagai spam tanpa await.
    Return True jika klaim berhasil (gate ini yang pertama).
    Return False jika found sudah di-set atau pesan sudah di-handle gate lain.

    Aman tanpa lock karena asyncio single-threaded — tidak ada coroutine lain
    yang bisa menyela di antara cek is_message_handled dan mark_message_handled
    selama tidak ada await di antaranya.
    """
    if found.is_set():
        return False
    if is_message_handled(cid, mid):
        found.set()  # sinkronkan event
        return False
    mark_message_handled(cid, mid)
    found.set()
    return True


# ─────────────────────────────────────────────────────────────────────────────
#  Gate A — Regex global + lokal
# ─────────────────────────────────────────────────────────────────────────────

async def _gate_regex(
    client, message, cfg, content, cid, uid, mid,
    regex_safe, teks_super_clean,
    found, delete_queue,
    is_message_handled, mark_message_handled,
    insert_group_action_log, check_and_punish,
    _get_global_patterns, _get_local_patterns,
    match_with_leet, _trigger_passive_learn_spam,
    VIOLATION_REGEX_GLOBAL, VIOLATION_REGEX_GRUP,
) -> None:
    if found.is_set():
        return

    for pat, raw in await _get_global_patterns():
        if found.is_set():
            return
        if match_with_leet(pat, regex_safe) or (teks_super_clean and pat.search(teks_super_clean)):
            if not _try_claim(found, cid, mid, is_message_handled, mark_message_handled):
                return
            await delete_queue.put((cid, [mid]))
            asyncio.create_task(insert_group_action_log(
                cid, "HAPUS", f"Filter Regex Global — pola: {raw[:50]}",
                uid, message.from_user.first_name or str(uid), content[:100],
                jenis=VIOLATION_REGEX_GLOBAL,
            ))
            asyncio.create_task(check_and_punish(client, message, "filter kata global", content[:100]))
            _trigger_passive_learn_spam(content, confidence=1.0)
            return

    if found.is_set():
        return

    for pat, raw in await _get_local_patterns(cid):
        if found.is_set():
            return
        if match_with_leet(pat, regex_safe) or (teks_super_clean and pat.search(teks_super_clean)):
            if not _try_claim(found, cid, mid, is_message_handled, mark_message_handled):
                return
            await delete_queue.put((cid, [mid]))
            asyncio.create_task(insert_group_action_log(
                cid, "HAPUS", f"Filter Regex Grup — pola: {raw[:50]}",
                uid, message.from_user.first_name or str(uid), content[:100],
                jenis=VIOLATION_REGEX_GRUP,
            ))
            asyncio.create_task(check_and_punish(client, message, "filter kata grup", content[:100]))
            _trigger_passive_learn_spam(content, confidence=1.0)
            return


# ─────────────────────────────────────────────────────────────────────────────
#  Gate B — Link detector
# ─────────────────────────────────────────────────────────────────────────────

async def _gate_link(
    client, message, cfg, content, cid, uid, mid,
    found, delete_queue,
    is_message_handled, mark_message_handled,
    insert_group_action_log, check_and_punish,
    _has_url_entity,
    VIOLATION_LINK_PESAN,
) -> None:
    if found.is_set():
        return
    if not cfg.get("anti_link", True):
        return
    if not _has_url_entity(message):
        return
    if not _try_claim(found, cid, mid, is_message_handled, mark_message_handled):
        return
    await delete_queue.put((cid, [mid]))
    asyncio.create_task(insert_group_action_log(
        cid, "HAPUS", "Link Detector — URL/tautan aktif dalam pesan",
        uid, message.from_user.first_name or str(uid), content[:100],
        jenis=VIOLATION_LINK_PESAN,
    ))
    asyncio.create_task(check_and_punish(client, message, "link dalam pesan", content[:100]))


# ─────────────────────────────────────────────────────────────────────────────
#  Gate C — Anti duplikasi lokal
# ─────────────────────────────────────────────────────────────────────────────

async def _gate_local_dup(
    client, message, cfg, content, cid, uid, mid,
    norm, now_ts, now_dt,
    found, messages_db, delete_queue,
    is_message_handled, mark_message_handled,
    insert_group_action_log, check_and_punish,
    get_local_mute, has_warned_user, mark_warned_user,
    auto_delete_reply, send_group_notice,
    VIOLATION_DUPLIKAT_LOKAL,
) -> None:
    import hashlib
    from pyrogram.enums import ParseMode
    from rapidfuzz import fuzz

    if found.is_set():
        return
    if cfg.get("local") is not True:
        return
    if message.via_bot:
        return
    if (1 <= len(content) <= 3) or content.isdigit():  # is_short
        return

    # Cek mute aktif
    mute_rec = await get_local_mute(cid, uid)
    if found.is_set():
        return
    if mute_rec.get("muted_until", 0.0) > now_ts:
        if not _try_claim(found, cid, mid, is_message_handled, mark_message_handled):
            return
        await delete_queue.put((cid, [mid]))
        return

    spam_limit = max(1, min(5, int(cfg.get("local_spam_limit", 1))))

    matched_old = None
    async for old in messages_db.find(
        {"chat_id": cid, "user_id": uid, "type": "local_track"}
    ).sort("time", -1).limit(spam_limit):
        if found.is_set():
            return
        old_norm = old.get("norm_txt", "")
        if not old_norm:
            continue
        if fuzz.ratio(norm, old_norm) >= 90:
            if (now_ts - old["time"]) < cfg["expiry"]:
                matched_old = old
                break

    if found.is_set():
        return

    if matched_old is not None:
        if not _try_claim(found, cid, mid, is_message_handled, mark_message_handled):
            return
        await delete_queue.put((cid, [matched_old["msg_id"], mid]))
        asyncio.create_task(insert_group_action_log(
            cid, "HAPUS", "Anti-Spam Duplikat Lokal — pesan mirip dikirim berulang",
            uid, message.from_user.first_name or str(uid), content[:100],
            jenis=VIOLATION_DUPLIKAT_LOKAL,
        ))
        asyncio.create_task(check_and_punish(client, message, "spam duplikat lokal", content[:100]))

        if not await has_warned_user(cid, uid, "dup"):
            msg_warn = await send_group_notice(
                client, cid,
                f"{message.from_user.mention} jangan kirim pesan yang sama",
                notice_kind="warn_dup",
                parse_mode=ParseMode.HTML,
                reply_to_message_id=mid,
            )
            if msg_warn is not None:
                asyncio.create_task(auto_delete_reply([msg_warn], delay=5))
            asyncio.create_task(mark_warned_user(cid, uid, "dup"))

        await messages_db.delete_one({"_id": matched_old["_id"]})
        new_id = f"loc_{cid}_{uid}_{hashlib.md5(content.encode()).hexdigest()}_{int(now_ts*1000)}"
        await messages_db.insert_one({
            "_id": new_id, "time": now_ts, "msg_id": mid,
            "chat_id": cid, "user_id": uid, "norm_txt": norm,
            "raw_hash": hashlib.md5(content.encode()).hexdigest(),
            "type": "local_track", "createdAt": now_dt,
        })
        return

    # Pesan bersih lokal → simpan, trim riwayat
    new_id = f"loc_{cid}_{uid}_{mid}_{int(now_ts * 1000)}"
    await messages_db.insert_one({
        "_id": new_id, "time": now_ts, "msg_id": mid,
        "chat_id": cid, "user_id": uid, "norm_txt": norm,
        "raw_hash": hashlib.md5(content.encode()).hexdigest(),
        "type": "local_track", "createdAt": now_dt,
    })
    # FIX: limit(spam_limit + 1) — tidak tarik semua dokumen ke memori
    all_docs = [d async for d in messages_db.find(
        {"chat_id": cid, "user_id": uid, "type": "local_track"}
    ).sort("time", -1).limit(spam_limit + 1)]
    if len(all_docs) > spam_limit:
        old_ids = [d["_id"] for d in all_docs[spam_limit:]]
        await messages_db.delete_many({"_id": {"$in": old_ids}})


# ─────────────────────────────────────────────────────────────────────────────
#  Gate D — Anti gcast global
# ─────────────────────────────────────────────────────────────────────────────

async def _gate_gcast(
    client, message, cfg, content, cid, uid, mid,
    now_ts, now_dt,
    found, messages_db, delete_queue,
    is_message_handled, mark_message_handled,
    insert_group_action_log, check_and_punish,
    get_config,
    GLOBAL_EXPIRY, VIOLATION_GCAST_GLOBAL,
    _gcast_punish_other_group,
) -> None:
    import hashlib

    if found.is_set():
        return
    if cfg.get("global") is not True:
        return
    if (1 <= len(content) <= 3) or content.isdigit():  # is_short
        return

    # Simplify sebelum hash — agar variasi unicode/leet/spasi tetap terdeteksi
    # sebagai pesan yang sama. Tanpa ini, "s4ng3" dan "sange" dianggap beda.
    from core.regex_utils import simplify as _simplify_gcast
    content_norm = _simplify_gcast(content) or content  # fallback ke raw jika hasil kosong
    content_hash = hashlib.md5(content_norm.encode()).hexdigest()
    global_key   = f"glob_{uid}_{content_hash}"
    existing     = await messages_db.find_one({"_id": global_key})

    if found.is_set():
        return

    if existing and (now_ts - existing["time"]) < GLOBAL_EXPIRY:
        locs = existing.get("locations", [])
        locs = [loc for loc in locs if loc[0] != cid]
        locs.append([cid, mid])
        await messages_db.update_one(
            {"_id": global_key},
            {"$set": {"locations": locs, "time": now_ts, "createdAt": now_dt}},
        )

        unique_chats = {loc[0] for loc in locs}
        if len(unique_chats) > 1:
            if found.is_set():
                return
            if not _try_claim(found, cid, mid, is_message_handled, mark_message_handled):
                return
            n_chats = len(unique_chats)
            for loc_cid, loc_mid in locs:
                t_cfg = await get_config(loc_cid)
                if t_cfg.get("global") is True:
                    mark_message_handled(loc_cid, loc_mid)
                    await delete_queue.put((loc_cid, [loc_mid]))
                    asyncio.create_task(insert_group_action_log(
                        loc_cid, "HAPUS",
                        f"Anti-Broadcast Gcast Global — disebar ke {n_chats} grup sekaligus",
                        uid, message.from_user.first_name or str(uid), content[:100],
                        jenis=VIOLATION_GCAST_GLOBAL,
                    ))
                    if loc_cid == cid:
                        asyncio.create_task(check_and_punish(
                            client, message, "anti-gcast global", content[:100]
                        ))
                    else:
                        asyncio.create_task(_gcast_punish_other_group(
                            client, loc_cid, uid, content[:100]
                        ))
    else:
        await messages_db.update_one(
            {"_id": global_key},
            {"$set": {
                "time": now_ts, "createdAt": now_dt,
                "locations": [[cid, mid]],
            }},
            upsert=True,
        )


# ─────────────────────────────────────────────────────────────────────────────
#  Gate E — External mention (Telegram API — sequential, setelah paralel)
# ─────────────────────────────────────────────────────────────────────────────

async def _gate_mention(
    client, message, cfg, content, cid, uid, mid,
    found, delete_queue,
    is_message_handled, mark_message_handled,
    insert_group_action_log, check_and_punish,
    _is_external_mention,
    VIOLATION_MENTION_NON_MEMBER,
) -> None:
    if found.is_set():
        return
    # Master toggle — kalau anti_mention OFF, seluruh sub-fitur mati
    if not cfg.get("anti_mention", True):
        return
    if not message.entities:
        return

    await wait_global_flood_backoff()
    if found.is_set():
        return

    try:
        _mention_result = await asyncio.wait_for(
            _is_external_mention(client, message, cfg),
            timeout=_MENTION_TIMEOUT,
        )
        is_ext, mention_kind, mention_uname = _mention_result
    except asyncio.TimeoutError:
        is_ext = False
        mention_kind = mention_uname = None
        print(f"[antispam_queue] ⚠️  Timeout mention check mid={mid} cid={cid}")
    except FloodWait as fw:
        set_global_flood_backoff(fw.value)
        is_ext = False
        mention_kind = mention_uname = None
        print(f"[antispam_queue] ⚠️  FloodWait {fw.value}s saat mention check, skip")
    except Exception as e:
        is_ext = False
        mention_kind = mention_uname = None
        print(f"[antispam_queue] ⚠️  Error mention check: {e}")

    if not is_ext or found.is_set():
        return
    if not _try_claim(found, cid, mid, is_message_handled, mark_message_handled):
        return

    # Buat alasan detail berdasarkan jenis mention
    _kind_label = {
        "non_akun": "Username tidak valid / akun tidak ditemukan",
        "channel":  "Username milik channel Telegram",
        "grup":     "Username milik grup / supergroup Telegram",
        "akun":     "Akun user — bukan anggota grup ini",
    }
    _uname_str   = f"@{mention_uname}" if mention_uname else "—"
    _alasan_str  = f"Mention {_uname_str} — {_kind_label.get(mention_kind or '', 'bukan anggota grup')}"

    await delete_queue.put((cid, [mid]))
    asyncio.create_task(insert_group_action_log(
        cid, "HAPUS", _alasan_str,
        uid, message.from_user.first_name or str(uid), content[:100],
        jenis=VIOLATION_MENTION_NON_MEMBER,
    ))
    asyncio.create_task(check_and_punish(client, message, "mention pengguna luar", content[:100]))


# ─────────────────────────────────────────────────────────────────────────────
#  _process_detection — orkestrasi utama
# ─────────────────────────────────────────────────────────────────────────────

async def _process_detection_no_perm(client, message, cfg: dict, cid: int, uid: int) -> None:
    """
    Dijalankan saat bot tidak punya izin delete+restrict di grup ini.
    Hanya menjalankan 2 hal yang tetap bermanfaat untuk grup lain:
      1. Anti-gcast global — record hash pesan untuk deteksi lintas grup
      2. Mention record    — simpan data username ke global/member cache
    Tidak ada hapus pesan, tidak ada mute, tidak ada log ke grup ini.
    """
    from database import messages_db, GLOBAL_EXPIRY
    from core.regex_utils import simplify as _simplify

    msg_text = (message.text or message.caption or "").strip()
    if not msg_text:
        return

    # ── 1. Anti-gcast record (tanpa eksekusi) ────────────────────────────────
    if cfg.get("global") is True:
        try:
            import hashlib, time as _time
            from core.regex_utils import simplify as _s
            _norm = _s(msg_text) or msg_text
            _hash = hashlib.md5(_norm.encode()).hexdigest()
            _key  = f"glob_{uid}_{_hash}"
            _now  = _time.time()
            _loc  = {"chat_id": cid, "ts": _now}
            await messages_db.update_one(
                {"_id": _key},
                {"$set": {"time": _now}, "$addToSet": {"locations": _loc}},
                upsert=True,
            )
        except Exception:
            pass

    # ── 2. Mention record (tanpa eksekusi hapus) ──────────────────────────────
    if cfg.get("anti_mention") and message.entities:
        try:
            from database import (
                mention_global_get, mention_global_set,
                mention_cache_get_by_username, mention_cache_set,
                mention_wl_get,
            )
            from pyrogram.enums import MessageEntityType
            whitelist = set(await mention_wl_get(cid))
            for _ent in message.entities:
                if _ent.type != MessageEntityType.MENTION:
                    continue
                _uname = msg_text[_ent.offset:_ent.offset + _ent.length].lstrip("@").lower()
                if not _uname or _uname in ("botfather", "telegram", "admin"):
                    continue
                if _uname in whitelist:
                    continue
                # Hanya record jika belum ada di global cache
                _gdoc = await mention_global_get(_uname)
                if _gdoc is None:
                    # Resolve tipe via API untuk disimpan ke global cache
                    try:
                        from pyrogram.enums import ChatType
                        from pyrogram.errors import PeerIdInvalid, RPCError
                        _chat = await client.get_chat(_uname)
                        if _chat.type == ChatType.CHANNEL:
                            import asyncio as _aio
                            _aio.create_task(mention_global_set(_uname, "channel"))
                        elif _chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
                            import asyncio as _aio
                            _aio.create_task(mention_global_set(_uname, "grup"))
                        # Akun biasa → record ke per-grup cache
                        elif _chat.type == ChatType.PRIVATE:
                            import asyncio as _aio
                            _aio.create_task(mention_cache_set(cid, _chat.id, False, username=_uname))
                    except (PeerIdInvalid, RPCError, KeyError, ValueError):
                        import asyncio as _aio
                        _aio.create_task(mention_global_set(_uname, "non_akun"))
                    except Exception:
                        pass
        except Exception:
            pass


async def _process_detection(client: "Client", message: "Message") -> None:
    import time as _time
    from datetime import datetime

    from database import (
        messages_db, get_config, delete_queue, TZ_WIB, auto_delete_reply,
        mark_message_handled, is_message_handled,
        get_local_mute,
        insert_group_action_log,
        has_warned_user, mark_warned_user,
        GLOBAL_EXPIRY,
    )
    from core.regex_utils import simplify, remove_mentions_for_regex, match_with_leet
    from core.punishment import check_and_punish
    from core.group_notify import send_group_notice
    from plugins.nexus.engine import pipeline_pembersihan
    from core.violation_types import (
        VIOLATION_REGEX_GLOBAL, VIOLATION_REGEX_GRUP,
        VIOLATION_MENTION_NON_MEMBER, VIOLATION_LINK_PESAN,
        VIOLATION_DUPLIKAT_LOKAL, VIOLATION_GCAST_GLOBAL,
    )
    from plugins.filters.antispam import (
        _get_global_patterns,
        _get_local_patterns,
        _is_external_mention,
        _has_url_entity,
        _trigger_passive_learn_spam,
        _gcast_punish_other_group,
    )

    cid = message.chat.id
    uid = message.from_user.id
    mid = message.id

    if is_message_handled(cid, mid):
        return

    from database import check_bot_permissions
    if not await check_bot_permissions(client, cid):
        return

    content = (message.text or message.caption or "").strip()
    if not content or content.startswith("/"):
        return

    cfg = await get_config(cid)

    # VIP bio bypass — sequential, sebelum semua gate
    _vip_text = (cfg.get("bio_vip_text") or "").strip()
    if _vip_text and cfg.get("bio_check"):
        try:
            from plugins.filters.bio import _check_vip_bio
            if await _check_vip_bio(cid, uid, _vip_text):
                return
        except Exception as _e:
            print(f"[antispam_queue] VIP bio check error uid={uid} cid={cid}: {_e}")

    now_ts = _time.time()
    now_dt = datetime.now(TZ_WIB)

    # Preprocessing CPU-only — jalankan sekali sebelum spawn gate
    regex_safe       = remove_mentions_for_regex(message)
    teks_super_clean = pipeline_pembersihan(content)
    # norm hanya dipakai gate C — hitung kondisional
    norm = simplify(content) if cfg.get("local") is True else ""

    # ── Event koordinasi: gate pertama yang klaim set found=True ─────────────
    found = asyncio.Event()

    # ── 4 Gate paralel ───────────────────────────────────────────────────────
    await asyncio.gather(
        _gate_regex(
            client, message, cfg, content, cid, uid, mid,
            regex_safe, teks_super_clean, found, delete_queue,
            is_message_handled, mark_message_handled,
            insert_group_action_log, check_and_punish,
            _get_global_patterns, _get_local_patterns,
            match_with_leet, _trigger_passive_learn_spam,
            VIOLATION_REGEX_GLOBAL, VIOLATION_REGEX_GRUP,
        ),
        _gate_link(
            client, message, cfg, content, cid, uid, mid,
            found, delete_queue,
            is_message_handled, mark_message_handled,
            insert_group_action_log, check_and_punish,
            _has_url_entity, VIOLATION_LINK_PESAN,
        ),
        _gate_local_dup(
            client, message, cfg, content, cid, uid, mid,
            norm, now_ts, now_dt, found, messages_db, delete_queue,
            is_message_handled, mark_message_handled,
            insert_group_action_log, check_and_punish,
            get_local_mute, has_warned_user, mark_warned_user,
            auto_delete_reply, send_group_notice,
            VIOLATION_DUPLIKAT_LOKAL,
        ),
        _gate_gcast(
            client, message, cfg, content, cid, uid, mid,
            now_ts, now_dt, found, messages_db, delete_queue,
            is_message_handled, mark_message_handled,
            insert_group_action_log, check_and_punish,
            get_config,
            GLOBAL_EXPIRY, VIOLATION_GCAST_GLOBAL,
            _gcast_punish_other_group,
        ),
        return_exceptions=True,
    )

    # ── Gate E: mention — hanya jika semua gate paralel miss ─────────────────
    if found.is_set():
        return

    await _gate_mention(
        client, message, cfg, content, cid, uid, mid,
        found, delete_queue,
        is_message_handled, mark_message_handled,
        insert_group_action_log, check_and_punish,
        _is_external_mention, VIOLATION_MENTION_NON_MEMBER,
    )
