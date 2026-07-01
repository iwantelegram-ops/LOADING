"""
plugins/filters/antispam.py
────────────────────────────
Filter utama pesan grup:
  1. Regex global & lokal  (Owner Regex — TANPA pengaruh Whitelist Nexus)
  2. External mention
  3. Link detector
  4. Anti duplikasi lokal (per user per grup) — RAM flood-counter (PROTEKSI C)
     + jalur fuzzy di core/antispam_queue.py (sort/limit by local_spam_limit).
     [DIHAPUS] Fast-path exact-match-bypass (_exact_match_local_bypass) yang
     dulu ada di sini — perannya (deteksi kalimat 100% identik berulang)
     sekarang diwakili oleh fitur DETEKSI UBOT (core/ubot_detect.py +
     plugins/filters/ubot_detect_filter.py), yang memakai memori Mongo
     terpisah (ubot_sentence_tracker) dan independen dari toggle "local".
  5. Anti duplikasi global (anti-gcast lintas grup) — PROTEKSI MASSAL ANTI-CLONE

SISTEM LOGGING:
  Telah dihubungkan secara penuh dengan plugins.commands.log (log_spam_lokal)
  sehingga setiap tindakan Fast-Path RAM langsung dilaporkan ke log worker/channel.
────────────────────────────
TOGGLE-DRIVEN DETECTION:
  Setiap fitur deteksi (bukan hanya hukuman) dimatikan sepenuhnya saat toggle OFF.
  - global OFF  → PROTEKSI A & B (RAM mass-burst) tidak berjalan sama sekali
  - local OFF   → PROTEKSI C (RAM per-user) tidak berjalan sama sekali
  - Logika detection_queue juga mengikuti toggle masing-masing fitur
"""

import os
import re
import time
import asyncio
import hashlib
from datetime import datetime
from pyrogram import Client, filters
from pyrogram.enums import MessageEntityType
from pyrogram.errors import UserNotParticipant, PeerIdInvalid, RPCError

LOG_CHANNEL = int(os.environ.get("LOG_CHANNEL", 0))

from database import (
    regex_db, get_config, is_admin, db,
    mark_message_handled, is_message_handled,
    get_local_mute, reset_local_mute,
    insert_group_action_log,
    check_bot_permissions,
)
from core.regex_utils import simplify, remove_mentions_for_regex, match_with_leet

# ── IMPOR FUNGSI HUKUMAN & LOG BAWAAN ANDA ───────────────────────────────────
from core.punishment import check_and_punish
from plugins.commands.log import log_spam_lokal, log_duplikat_lokal, log_mass_flood

group_regex_db = db["regex_per_group"]
free_col       = db["free_per_group"]

# ── 1. Cache Per-User (Bom Spam dari 1 Akun Tunggal) ──────────────────────────
_local_flood_cache: dict[int, dict[int, tuple[str, float, int]]] = {}
_FLOOD_WINDOW   = 5.0  
_MAX_DUPLICATE  = 2    

# ── 2. Cache Lintas-User (Serangan Massal Banyak Akun Kloning / Userbot) ──────
_global_text_tracker: dict[int, dict[str, list[float]]] = {}
_global_text_blacklist: dict[int, dict[str, float]] = {}

_MASS_BURST_WINDOW = 1.5  
_MASS_BURST_LIMIT  = 3    
_LOCK_DURATION     = 10.0 

# ── Cache regex ───────────────────────────────────────────────────────────────
_regex_cache:     list  = []
_regex_cache_ts:  float = 0.0
_local_regex_cache: dict[int, tuple[list, float]] = {}
REGEX_TTL = 300

_URL_ENTITY_TYPES = {MessageEntityType.URL, MessageEntityType.TEXT_LINK}


def _has_url_entity(message) -> bool:
    entities = list(message.entities or []) + list(message.caption_entities or [])
    return any(e.type in _URL_ENTITY_TYPES for e in entities)


async def _get_global_patterns():
    global _regex_cache, _regex_cache_ts
    now = time.monotonic()
    if now - _regex_cache_ts < REGEX_TTL:
        return _regex_cache
    patterns = []
    async for doc in regex_db.find({"pattern": {"$exists": True}}):
        try:
            raw = doc.get("raw") or doc.get("pattern", "")
            patterns.append((re.compile(doc["pattern"], re.IGNORECASE), raw))
        except Exception:
            pass
    _regex_cache = patterns
    _regex_cache_ts = now
    return _regex_cache


async def _get_local_patterns(chat_id: int):
    now = time.monotonic()
    hit = _local_regex_cache.get(chat_id)
    if hit and (now - hit[1]) < REGEX_TTL:
        return hit[0]
    patterns = []
    async for doc in group_regex_db.find({"chat_id": chat_id}):
        try:
            raw = doc.get("raw") or doc.get("pattern", "")
            patterns.append((re.compile(doc["pattern"], re.IGNORECASE), raw))
        except Exception:
            pass
    _local_regex_cache[chat_id] = (patterns, now)
    return patterns


def invalidate_local_regex_cache(chat_id: int) -> None:
    _local_regex_cache.pop(chat_id, None)


async def _is_external_mention(client: Client, message, cfg: dict) -> bool:
    """
    Deteksi apakah pesan mengandung mention yang dilarang di grup ini.

    Urutan cek per mention (@username):
      0. Whitelist grup → skip seluruh cek, lanjut entity berikutnya
      1. Global non-akun cache → langsung return True (hapus) tanpa cek lagi
      2. Global channel/grup cache → cek toggle batasi_channel / batasi_grup
      3. Per-grup member cache (existing) → cek toggle batasi_akun
      4. API call → resolve tipe entity → simpan ke koleksi yang tepat

    Toggle sub-fitur (dari cfg grup):
      mention_batasi_akun    — batasi mention ke user non-member (default True)
      mention_batasi_channel — batasi mention ke channel (default True)
      mention_batasi_grup    — batasi mention ke grup/supergroup (default True)

    Whitelist berlaku untuk semua jenis entity — tidak per-jenis.
    """
    if not message.entities:
        return False

    # Ambil sub-toggle dari config grup
    batasi_akun    = cfg.get("mention_batasi_akun",    True)
    batasi_channel = cfg.get("mention_batasi_channel", True)
    batasi_grup    = cfg.get("mention_batasi_grup",    True)

    # Kalau semua sub-toggle OFF → tidak ada yang perlu dicek
    if not batasi_akun and not batasi_channel and not batasi_grup:
        return False

    msg_text = message.text or message.caption or ""
    cid = message.chat.id

    # Ambil whitelist sekali di luar loop
    from database import (
        mention_cache_get_by_uid, mention_cache_get_by_username,
        mention_cache_refresh_ttl, mention_cache_set,
        mention_global_get, mention_global_set,
        mention_wl_get,
    )
    whitelist = set(await mention_wl_get(cid))

    try:
        from monitor_bot_reference import check_member_via_monitor
        _monitor_available = True
    except Exception:
        _monitor_available = False

    for entity in message.entities:
        # Hanya proses @username mention biasa.
        # TEXT_MENTION di-skip — hanya bisa dilakukan ke member aktif.
        if entity.type != MessageEntityType.MENTION:
            continue

        uname = msg_text[entity.offset:entity.offset + entity.length].lstrip("@").lower()
        if not uname:
            continue

        # Skip username sistem Telegram
        if uname in ("botfather", "telegram", "admin"):
            continue

        # ── 0. Whitelist grup ────────────────────────────────────────────────
        if uname in whitelist:
            continue

        # ── 1. Global non-akun cache ─────────────────────────────────────────
        global_doc = await mention_global_get(uname)
        if global_doc is not None:
            kind = global_doc.get("kind")
            if kind == "non_akun":
                return True, "non_akun", uname
            elif kind == "channel" and batasi_channel:
                return True, "channel", uname
            elif kind == "grup" and batasi_grup:
                return True, "grup", uname
            elif kind in ("channel", "grup"):
                continue  # toggle OFF → skip
            # kind lain → fall through ke API

        # ── 2. Per-grup member cache (untuk akun biasa) ──────────────────────
        cached = await mention_cache_get_by_username(cid, uname)
        if cached is not None:
            asyncio.create_task(mention_cache_refresh_ttl(cid, username=uname))
            if not cached and batasi_akun:
                return True, "akun", uname
            continue

        # ── 3. API call → resolve tipe entity ────────────────────────────────
        try:
            from pyrogram.enums import ChatType
            chat_obj = await client.get_chat(uname)
            chat_type = chat_obj.type

            if chat_type == ChatType.PRIVATE:
                uid_obj = chat_obj.id
                try:
                    member = await client.get_chat_member(cid, uid_obj)
                    is_member = member is not None
                except (UserNotParticipant, PeerIdInvalid, RPCError, KeyError, ValueError):
                    is_member = False
                asyncio.create_task(mention_cache_set(cid, uid_obj, is_member, username=uname))
                if not is_member and batasi_akun:
                    return True, "akun", uname

            elif chat_type == ChatType.CHANNEL:
                asyncio.create_task(mention_global_set(uname, "channel"))
                if batasi_channel:
                    return True, "channel", uname

            elif chat_type in (ChatType.GROUP, ChatType.SUPERGROUP):
                asyncio.create_task(mention_global_set(uname, "grup"))
                if batasi_grup:
                    return True, "grup", uname

        except (PeerIdInvalid, RPCError, KeyError, ValueError):
            asyncio.create_task(mention_global_set(uname, "non_akun"))
            return True, "non_akun", uname
        except Exception:
            pass

    return False, None, None

def _trigger_passive_learn_spam(text: str, confidence: float = 1.0) -> None:
    try:
        from nexus.ai_core import nexus_ai_passive_observe
        asyncio.create_task(
            nexus_ai_passive_observe(text, True, confidence, force_learn=True)
        )
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
#  Main filter (group=2) — FAST-PATH RAM
# ─────────────────────────────────────────────────────────────────────────────
@Client.on_message((filters.group | filters.forum) & ~filters.service, group=2)
async def main_antispam_filter(client, message):
    if not message.from_user:
        return
    cid, uid, mid = message.chat.id, message.from_user.id, message.id

    if is_message_handled(cid, mid):
        return

    if await is_admin(client, cid, uid):
        return

    if await free_col.find_one({"user_id": uid, "chat_id": cid}):
        return

    content = (message.text or message.caption or "").strip()
    if not content or content.startswith("/"):
        return

    # ── Ambil config SEKALI di awal — semua fast-path RAM bergantung padanya ──
    cfg = await get_config(cid)

    # ── Cek izin bot ──────────────────────────────────────────────────────────
    # Kalau bot tidak punya delete_messages DAN restrict_members:
    #   • Anti-gcast global → tetap jalan (datanya berguna untuk grup lain)
    #   • Mention record    → tetap jalan (datanya berguna untuk grup lain)
    #   • Semua eksekusi lain (hapus, mute, dll) → STOP
    _has_perms = await check_bot_permissions(client, cid)
    if not _has_perms:
        # Tetap jalankan anti-gcast (record saja, tidak hapus)
        from core.antispam_queue import _process_detection_no_perm
        asyncio.create_task(_process_detection_no_perm(client, message, cfg, cid, uid))
        return
    global_on = cfg.get("global") is True
    local_on  = cfg.get("local")  is True

    content_hash = hashlib.md5(content.encode("utf-8", errors="ignore")).hexdigest()
    now_ts = time.time()

    # ── PROTEKSI A: Karantina RAM Sementara (Serangan Massal Banyak Akun) ──────
    # Hanya berjalan jika toggle global ON
    if global_on and cid in _global_text_blacklist and content_hash in _global_text_blacklist[cid]:
        if now_ts < _global_text_blacklist[cid][content_hash]:
            mark_message_handled(cid, mid)
            asyncio.create_task(check_and_punish(client, message, "MASS_FLOOD_BURST_RAM", content))
            asyncio.create_task(log_mass_flood(client, message, pola=content[:80], indikator="MASS_FLOOD_BURST_RAM"))
            asyncio.create_task(message.delete())
            return
        else:
            _global_text_blacklist[cid].pop(content_hash, None)

    # ── PROTEKSI B: Deteksi Serangan Massal Banyak Akun Kloning (Lintas User) ──
    # Tracking & eksekusi hanya jika toggle global ON
    if global_on:
        if cid not in _global_text_tracker:
            _global_text_tracker[cid] = {}

        if content_hash not in _global_text_tracker[cid]:
            _global_text_tracker[cid][content_hash] = []

        _global_text_tracker[cid][content_hash].append(now_ts)

        _global_text_tracker[cid][content_hash] = [
            ts for ts in _global_text_tracker[cid][content_hash]
            if (now_ts - ts) <= _MASS_BURST_WINDOW
        ]

        if len(_global_text_tracker[cid][content_hash]) >= _MASS_BURST_LIMIT:
            if cid not in _global_text_blacklist:
                _global_text_blacklist[cid] = {}

            _global_text_blacklist[cid][content_hash] = now_ts + _LOCK_DURATION

            mark_message_handled(cid, mid)
            asyncio.create_task(check_and_punish(client, message, "MASS_FLOOD_BURST_RAM", content))
            asyncio.create_task(log_mass_flood(client, message, pola=content[:80], indikator="MASS_FLOOD_BURST_RAM"))
            asyncio.create_task(message.delete())
            return

    # ── PROTEKSI C: Deteksi Duplikasi Tunggal Per-User ────────────────────────
    # Tracking & eksekusi hanya jika toggle local ON
    if local_on:
        if cid not in _local_flood_cache:
            _local_flood_cache[cid] = {}

        user_flood_data = _local_flood_cache[cid].get(uid)

        if user_flood_data:
            last_hash, last_time, duplicate_count = user_flood_data

            if last_hash == content_hash and (now_ts - last_time) < _FLOOD_WINDOW:
                duplicate_count += 1
                _local_flood_cache[cid][uid] = (content_hash, now_ts, duplicate_count)

                if duplicate_count >= _MAX_DUPLICATE:
                    mark_message_handled(cid, mid)
                    asyncio.create_task(check_and_punish(client, message, "LOCAL_FLOOD_RAM", content))
                    asyncio.create_task(log_duplikat_lokal(client, message, pola=content[:80], indikator="LOCAL_FLOOD_RAM"))
                    asyncio.create_task(message.delete())
                    return
            else:
                _local_flood_cache[cid][uid] = (content_hash, now_ts, 1)
        else:
            _local_flood_cache[cid][uid] = (content_hash, now_ts, 1)

    # ── Enqueue ke detection_queue (Untuk sistem antrean latar belakang bawaan) ──
    from core.antispam_queue import enqueue_for_detection
    await enqueue_for_detection(client, message)


async def _gcast_punish_other_group(
    client,
    chat_id: int,
    user_id: int,
    konten: str,
) -> None:
    from database import (
        get_local_mute, increment_local_spam, apply_local_mute,
        revert_failed_local_mute, insert_group_action_log,
    )
    from core.punishment import SPAM_MUTE_THRESHOLD
    from core.moderation_queue import queue_mute
    import time as _time
    now_ts = _time.time()
    mute_rec = await get_local_mute(chat_id, user_id)
    if mute_rec.get("muted_until", 0.0) > now_ts:
        return
    updated = await increment_local_spam(chat_id, user_id)
    consec  = updated.get("consec_spam", 1)
    if consec < SPAM_MUTE_THRESHOLD:
        return
    duration_secs, level_before = await apply_local_mute(chat_id, user_id)
    duration_min = duration_secs // 60

    async def _on_done(success: bool):
        if not success:
            await revert_failed_local_mute(chat_id, user_id, level_before)
            return
        try:
            from core.violation_types import VIOLATION_MUTE_ESKALASI
            await insert_group_action_log(
                chat_id, "MUTE",
                f"Mute {duration_min} mnt — 10x pelanggaran berulang (apapun jenisnya)",
                user_id, str(user_id), konten,
                jenis=VIOLATION_MUTE_ESKALASI,
            )
        except Exception:
            pass

    await queue_mute(chat_id, user_id, duration_secs, on_done=_on_done)


# ─────────────────────────────────────────────────────────────────────────────
#  group=10 — Tracker pesan bersih
# ─────────────────────────────────────────────────────────────────────────────
@Client.on_message((filters.group | filters.forum) & ~filters.service, group=10)
async def _clean_message_tracker(client, message):
    if not message.from_user or message.from_user.is_bot:
        return
    cid = message.chat.id
    mid = message.id
    uid = message.from_user.id

    if not is_message_handled(cid, mid):
        asyncio.create_task(_reset_mute_async(cid, uid))


async def _reset_mute_async(chat_id: int, user_id: int) -> None:
    try:
        await reset_local_mute(chat_id, user_id)
    except Exception:
        pass
