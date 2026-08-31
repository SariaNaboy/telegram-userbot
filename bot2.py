# -*- coding: utf-8 -*-
"""
bot2 — ربات دوم (همان Actions، اکانت/محل متفاوت)

- روی هر پست جدید کانال، سعی می‌کند کامنت دوم یا سوم باشد:
  صبر می‌کند تا حداقل یک کامنت بیرونی بیاید (count>=1) بعد 🦦🦦 می‌گذارد
- کامنت پست قبلی را پاک نمی‌کند
- بعد از کامنت، هویت به AmirAli می‌رود؛ بعد از ۱۰ دقیقه بدون پست جدید
  به حالت اول (Maya) برمی‌گردد
- پیام به ادمین: ۱۰ تا ۳۰ ثانیه بعد از کامنت
"""
import os
import time
import asyncio
import secrets
import random
import traceback
import logging
from io import BytesIO
from typing import List, Any
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

from pyrogram import Client, raw
from pyrogram.errors import FloodWait
from pyrogram.raw.types import (
    PeerUser,
    PeerChannel,
    InputPeerChannel,
    InputChannel,
    UpdateNewChannelMessage,
)
from pyrogram.raw.core import TLObject
from pyrogram.raw.core.primitives import Int

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION = os.environ["SESSION_STRING"]
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME") or os.getenv("admin_username")

# ===== کانفیگ ربات دوم =====
SOURCE_CHANNELS = {-1001985660752}        # کانال: 1985660752
COMMENT_GROUPS = {-1002866597350}   # گروه: 2866597350
DELETE_GROUPS = {-1002866597350}    # خود-حذفی
TRIGGER_WORDS = {
    "گزارش", "report", "@admin", "صیک", "سیک",
    "اخطار", "بن", "سکوت", "ban", "mute",
}
COMMENT_TEXT = "🦦🦦"
WAIT_FOR_FIRST_COMMENT = int(os.getenv("WAIT_FOR_FIRST_COMMENT", "180"))  # تا ۳ دقیقه صبر برای دوم/سوم
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "15.0"))
PROFILE_REVERT_SECONDS = 600             # ۱۰ دقیقه بعد از آخرین پست -> برگشت به Maya
ADMIN_NOTIFY_MIN = 10                    # ۱۰ ثانیه بعد از کامنت
ADMIN_NOTIFY_MAX = 30                    # ۳۰ ثانیه بعد از کامنت
ADMIN_NOTIFY_WORDS = ["شد", "ثبت", "انجام", "اوکی", "رفت"]

# ===== هویت خودکار (مثل main.py) =====
class InputPrivacyKeyAbout(TLObject):
    """inputPrivacyKeyAbout#3823cc40 = InputPrivacyKey;"""
    __slots__: List[str] = []
    ID = 0x3823CC40
    QUALNAME = "types.InputPrivacyKeyAbout"
    def __init__(self) -> None:
        pass
    @staticmethod
    def read(b: BytesIO, *args: Any) -> "InputPrivacyKeyAbout":
        return InputPrivacyKeyAbout()
    def write(self, *args: Any) -> bytes:
        b = BytesIO()
        b.write(Int(self.ID, False))
        return b.getvalue()


PROFILE_AMIRALI_NAME = "⒜⒨⒤⒭⒜⒧⒤"
PROFILE_AMIRALI_USERNAME = "Amirali126868"
PROFILE_MAYA_NAME = "Maya"
PROFILE_MAYA_USERNAME = ""

app = Client(
    "bot2",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION,
    sleep_threshold=0,
    workers=16,
)

peer_cache = {}
last_seen_channel_post = {}   # chat -> آخرین پست دیده‌شده
last_seen_group_msg = {}      # chat -> آخرین پیام گروه (برای group-poll)
comment_attempted = set()     # (chat, root_id) — ددپلیکیت: فقط یک بار
waiting_roots = {}
thread_watchers = {}
recently_mapped = {}
last_post_detected = None
profile_mode = "maya"
MY_USER_ID = None
my_messages = defaultdict(set)


def chat_id_from_channel_id(channel_id: int) -> int:
    return -(1_000_000_000_000 + channel_id)


async def get_channel_peers(chat_id: int):
    cached = peer_cache.get(chat_id)
    if cached is not None:
        return cached
    peer = await app.resolve_peer(chat_id)
    if not isinstance(peer, InputPeerChannel):
        raise TypeError(f"{chat_id} is not a channel/supergroup")
    channel = InputChannel(channel_id=peer.channel_id, access_hash=peer.access_hash)
    peer_cache[chat_id] = (peer, channel)
    return peer, channel


async def index_recent_own_messages(chat_id: int):
    """در استارت، پیام‌های قبلی خودمان را در گروه می‌شمارد تا خود-حذفی کار کند."""
    indexed = 0
    try:
        async for item in app.get_chat_history(chat_id, limit=500):
            sender = getattr(item, "from_user", None)
            if getattr(item, "outgoing", False) or (
                sender is not None and sender.id == MY_USER_ID
            ):
                my_messages[chat_id].add(item.id)
                indexed += 1
        print(f"[OWN HISTORY INDEXED] chat={chat_id} messages={indexed}", flush=True)
    except Exception as exc:
        print(f"[OWN HISTORY INDEX ERROR] chat={chat_id}: {exc!r}", flush=True)


async def target_is_mine(chat_id: int, message_id: int) -> bool:
    if message_id in my_messages[chat_id]:
        return True
    try:
        target = await app.get_messages(chat_id, message_id)
    except Exception as exc:
        print(f"[TARGET LOOKUP ERROR] {chat_id}/{message_id}: {exc!r}", flush=True)
        return False
    if not target or getattr(target, "empty", False):
        return False
    sender = getattr(target, "from_user", None)
    mine = bool(getattr(target, "outgoing", False)) or (
        sender is not None and sender.id == MY_USER_ID
    )
    if mine:
        my_messages[chat_id].add(message_id)
    return mine


async def delete_now(chat_id: int, message_id: int):
    try:
        peer, _ = await get_channel_peers(chat_id)
        await app.invoke(raw.functions.channels.DeleteMessages(channel=peer, id=[message_id]))
        my_messages[chat_id].discard(message_id)
        print(f"[DELETED] {chat_id}/{message_id}", flush=True)
    except FloodWait as exc:
        print(f"[DELETE FLOOD] wait={exc.value}s {chat_id}/{message_id}", flush=True)
        await asyncio.sleep(exc.value + 1)
        try:
            peer, _ = await get_channel_peers(chat_id)
            await app.invoke(raw.functions.channels.DeleteMessages(channel=peer, id=[message_id]))
            my_messages[chat_id].discard(message_id)
            print(f"[DELETED AFTER FLOOD] {chat_id}/{message_id}", flush=True)
        except Exception as retry_exc:
            print(f"[DELETE RETRY ERROR] {chat_id}/{message_id}: {retry_exc!r}", flush=True)
    except Exception as exc:
        print(f"[DELETE ERROR] {chat_id}/{message_id}: {exc!r}", flush=True)


async def notify_admin(text: str):
    if not ADMIN_USERNAME:
        return
    try:
        await app.send_message(ADMIN_USERNAME, text)
        print("[ADMIN NOTIFIED]", flush=True)
    except Exception as exc:
        print(f"[ADMIN NOTIFY ERROR] {exc!r}", flush=True)


async def notify_admin_delayed():
    """۱۰ تا ۳۰ ثانیه بعد از کامنت، یک کلمه به ادمین."""
    try:
        delay = random.randint(ADMIN_NOTIFY_MIN, ADMIN_NOTIFY_MAX)
        print(f"[ADMIN NOTIFY SCHEDULED] in {delay}s", flush=True)
        await asyncio.sleep(delay)
        await notify_admin(random.choice(ADMIN_NOTIFY_WORDS))
    except Exception as exc:
        print(f"[ADMIN NOTIFY DELAYED ERROR] {exc!r}", flush=True)


async def set_privacy_rule(key, allow_all: bool):
    rule = (
        raw.types.InputPrivacyValueAllowAll()
        if allow_all
        else raw.types.InputPrivacyValueDisallowAll()
    )
    return await app.invoke(raw.functions.account.SetPrivacy(key=key, rules=[rule]))


async def apply_profile_amirali():
    global profile_mode
    try:
        await app.update_profile(first_name=PROFILE_AMIRALI_NAME)
        await set_privacy_rule(raw.types.InputPrivacyKeyProfilePhoto(), allow_all=False)
        await set_privacy_rule(InputPrivacyKeyAbout(), allow_all=False)
        profile_mode = "amirali"
        print("[PROFILE -> AmirAli] name=⒜⒨⒤⒭⒜⒧⒤ photo=hidden bio=hidden", flush=True)
    except Exception as exc:
        print(f"[PROFILE AMIRALI ERROR] {exc!r}", flush=True)
        print(traceback.format_exc(), flush=True)


async def apply_profile_maya():
    global profile_mode
    try:
        await app.update_profile(first_name=PROFILE_MAYA_NAME)
        await set_privacy_rule(raw.types.InputPrivacyKeyProfilePhoto(), allow_all=True)
        await set_privacy_rule(InputPrivacyKeyAbout(), allow_all=True)
        profile_mode = "maya"
        print("[PROFILE -> Maya] name=Maya photo=public bio=public", flush=True)
    except Exception as exc:
        print(f"[PROFILE MAYA ERROR] {exc!r}", flush=True)
        print(traceback.format_exc(), flush=True)


async def current_profile_is_amirali():
    """وضعیت واقعی هویت را از سرور تلگرام می‌پرسد (نه از حافظه).

    Returns:
        True  -> الان واقعاً AmirAli است
        False -> الان AmirAli نیست (Maya یا چیز دیگر)
        None  -> خطا در دریافت (نامشخص)
    """
    try:
        me = await app.get_me()
        name = (me.first_name or "")
        is_amirali = (name == PROFILE_AMIRALI_NAME)
        print(
            f"[GET_ME] actual_name={name!r} is_amirali={is_amirali}",
            flush=True,
        )
        return is_amirali
    except Exception as exc:
        print(f"[GET_ME ERROR] {exc!r}", flush=True)
        return None


async def profile_watchdog():
    global profile_mode
    while True:
        try:
            if (
                profile_mode == "amirali"
                and last_post_detected is not None
                and time.monotonic() - last_post_detected >= PROFILE_REVERT_SECONDS
            ):
                # اول ببین الان واقعاً AmirAli است یا نه (شاید کاربر دستی عوض کرده باشد)
                is_amirali = await current_profile_is_amirali()
                if is_amirali is True:
                    print(f"[PROFILE TIMER] {PROFILE_REVERT_SECONDS}s بدون پست جدید -> بازگشت به Maya", flush=True)
                    await apply_profile_maya()
                elif is_amirali is False:
                    # کاربر دستی عوض کرده — فقط وضعیت حافظه را هماهنگ کن
                    profile_mode = "maya"
                    print("[PROFILE] already Maya (verified via get_me)", flush=True)
                else:
                    # خطای get_me — fallback به حافظه
                    if profile_mode == "amirali":
                        await apply_profile_maya()
        except Exception as exc:
            print(f"[PROFILE WATCHDOG ERROR] {exc!r}", flush=True)
        await asyncio.sleep(10)


async def normalize_old_comments():
    """پیام‌های قبلی *خودم* را از سرور می‌گردد (search_messages با from_user=me)
    و هر کدام که متنشان 🦦🦦 نیست به 🦦🦦 ادیت می‌کند.
    این روش حتی به پیام‌های خیلی قدیمی‌تر هم می‌رسد — برخلاف اسکن history."""
    total_mine = edited = skipped = 0
    for chat_id in COMMENT_GROUPS | DELETE_GROUPS:
        try:
            async for item in app.search_messages(chat_id, from_user="me"):
                total_mine += 1
                try:
                    text = getattr(item, "text", None)
                    if text is None or text == COMMENT_TEXT:
                        skipped += 1
                        continue
                    try:
                        await app.edit_message_text(chat_id, item.id, COMMENT_TEXT)
                        edited += 1
                        print(f"[NORMALIZED] {chat_id}/{item.id} -> {COMMENT_TEXT}", flush=True)
                        await asyncio.sleep(1)  # آروم، ضد flood
                    except FloodWait as exc:
                        print(f"[NORMALIZE FLOOD] wait={exc.value}s", flush=True)
                        await asyncio.sleep(exc.value + 1)
                    except Exception as exc:
                        # پیام خیلی قدیمی (۴۸ساعت) / پاک‌شده / MESSAGE_NOT_MODIFIED
                        print(f"[NORMALIZE SKIP] {chat_id}/{item.id}: {exc!r}", flush=True)
                        skipped += 1
                except Exception as exc:
                    print(f"[NORMALIZE ITEM ERROR] {exc!r}", flush=True)
        except Exception as exc:
            print(f"[NORMALIZE SEARCH ERROR] {chat_id}: {exc!r}", flush=True)
    print(f"[NORMALIZE DONE] own_msgs={total_mine} edited={edited} skipped={skipped}", flush=True)


async def send_comment(chat_id: int, root_message_id: int):
    """کامنت 🦦🦦 روی ریشه + نوتیف ادمین + تغییر هویت به AmirAli."""
    global last_post_detected
    try:
        peer, _ = await get_channel_peers(chat_id)
        print(f"[COMMENT ATTEMPT] chat={chat_id} root={root_message_id} text={COMMENT_TEXT!r}", flush=True)
        result = await app.invoke(
            raw.functions.messages.SendMessage(
                peer=peer,
                message=COMMENT_TEXT,
                random_id=secrets.randbits(63),
                reply_to_msg_id=root_message_id,
                no_webpage=True,
            )
        )
        print(f"[COMMENT SENT] chat={chat_id} root={root_message_id} type={type(result).__name__}", flush=True)
        last_post_detected = time.monotonic()
        asyncio.create_task(notify_admin_delayed())

        # چک واقعی از سرور: فقط اگر واقعاً AmirAli نیست، تغییر بده
        is_amirali = await current_profile_is_amirali()
        if is_amirali is False:
            await apply_profile_amirali()
        elif is_amirali is True:
            print("[PROFILE] already AmirAli (verified via get_me)", flush=True)
        else:
            # خطای get_me — fallback به حافظه
            if profile_mode != "amirali":
                await apply_profile_amirali()

        # پیام‌های قبلی خودمان را به 🦦🦦 یکدست کن (در پس‌زمینه؛ هیچ خطایی بات را نمی‌کشد)
        asyncio.create_task(normalize_old_comments())
    except FloodWait as exc:
        print(f"[COMMENT FLOOD] wait={exc.value}s", flush=True)
        await asyncio.sleep(exc.value + 1)
        try:
            peer, _ = await get_channel_peers(chat_id)
            await app.invoke(
                raw.functions.messages.SendMessage(
                    peer=peer,
                    message=COMMENT_TEXT,
                    random_id=secrets.randbits(63),
                    reply_to_msg_id=root_message_id,
                    no_webpage=True,
                )
            )
            print(f"[COMMENT SENT AFTER FLOOD] chat={chat_id} root={root_message_id}", flush=True)
        except Exception as retry_exc:
            print(f"[COMMENT RETRY ERROR] {chat_id}/{root_message_id}: {retry_exc!r}", flush=True)
    except Exception as exc:
        print(f"[COMMENT ERROR] {chat_id}/{root_message_id}: {exc!r}", flush=True)
        print(traceback.format_exc(), flush=True)


async def reserve_and_send(chat_id: int, root_message_id: int, reason: str):
    root_key = (chat_id, root_message_id)
    if root_key in comment_attempted:
        return
    comment_attempted.add(root_key)
    waiting_roots.pop(root_key, None)
    print(f"[COMMENT RESERVED] {root_key} reason={reason}", flush=True)
    await send_comment(chat_id, root_message_id)


async def get_replies_count(chat_id: int, root_message_id: int) -> int:
    """تعداد کامنت‌های ریشه را از روی خود پیام می‌خواند (نه GetReplies که MSG_ID_INVALID می‌داد).
    مقدار replies.replies روی Message ریشه همان تعداد کامنت‌های دیسکاشن است."""
    try:
        peer, channel = await get_channel_peers(chat_id)
        result = await app.invoke(
            raw.functions.channels.GetMessages(
                channel=channel, id=[raw.types.InputMessageID(id=root_message_id)]
            )
        )
        msgs = getattr(result, "messages", [])
        if msgs:
            m = msgs[0]
            if isinstance(m, raw.types.Message):
                reps = getattr(m, "replies", None)
                if reps is not None:
                    return int(getattr(reps, "replies", 0) or 0)
                return 0
    except FloodWait:
        raise
    except Exception as exc:
        print(f"[COUNT READ ERROR] {chat_id}/{root_message_id}: {exc!r} — fallback GetReplies", flush=True)
        # فالبک: همان متد قبلی
        return await app.get_discussion_replies_count(chat_id, root_message_id)
    return 0


async def watch_discussion_root(chat_id: int, root_message_id: int):
    """صبر می‌کند تا حداقل یک کامنت بیرونی بیاید (دوم/سوم شدن)، بعد کامنت می‌گذارد."""
    root_key = (chat_id, root_message_id)
    deadline = time.monotonic() + WAIT_FOR_FIRST_COMMENT
    last_count = None
    invalid_hits = 0
    try:
        while time.monotonic() < deadline:
            if root_key in comment_attempted:
                return
            try:
                count = await get_replies_count(chat_id, root_message_id)
                if count != last_count:
                    print(f"[WATCHER COUNT] {root_key} count={count}", flush=True)
                    last_count = count
                if count >= 1:
                    await reserve_and_send(chat_id, root_message_id, "watcher-count")
                    return
            except Exception as exc:
                print(f"[WATCHER COUNT ERROR] {root_key}: {exc!r}", flush=True)
                if "MSG_ID_INVALID" in repr(exc):
                    # اغلب موقتی است (تأخیر پروپگیشن تلگرام بعد از ساخته شدن ریشه) — تا ۱۰ بار تلاش کن
                    invalid_hits += 1
                    print(f"[WATCHER MSG_ID_INVALID {invalid_hits}/10] {root_key} — retry", flush=True)
                    if invalid_hits >= 10:
                        waiting_roots.pop(root_key, None)
                        print(f"[WATCHER GIVE UP] {root_key}", flush=True)
                        # پروب تشخیصی: علت را در لاگ بگذار (پیام پاک شده؟ دسترسی رفته؟)
                        try:
                            probe = await app.get_messages(chat_id, root_message_id)
                            probe_empty = getattr(probe, "empty", None) if probe else None
                            print(f"[GIVEUP PROBE] root message exists={probe is not None} empty={probe_empty}", flush=True)
                        except Exception as probe_exc:
                            print(f"[GIVEUP PROBE MSG ERROR] {probe_exc!r}", flush=True)
                        try:
                            member = await app.get_chat_member(chat_id, MY_USER_ID)
                            print(f"[GIVEUP PROBE] my member status={getattr(member, 'status', None)}", flush=True)
                        except Exception as memb_exc:
                            print(f"[GIVEUP MEMBER PROBE ERROR] {memb_exc!r}", flush=True)
                        return
                    await asyncio.sleep(4)
                    continue
                invalid_hits = 0
                if "FLOOD_WAIT" in repr(exc):
                    await asyncio.sleep(5)
            await asyncio.sleep(4)
        waiting_roots.pop(root_key, None)
        print(f"[WATCHER TIMEOUT] {root_key}", flush=True)
    finally:
        thread_watchers.pop(root_key, None)


async def observe_channel_post_discussion(source_chat_id: int, source_message_id: int):
    """پست کانال -> ریشه بحث -> شروع واچر دوم/سوم شدن."""
    global last_post_detected
    try:
        key = (source_chat_id, source_message_id)
        now = time.monotonic()
        if key in recently_mapped and now - recently_mapped[key] < 120:
            return True

        max_retries = 3
        for attempt in range(max_retries):
            try:
                root = await app.get_discussion_message(source_chat_id, source_message_id)
                break
            except FloodWait as exc:
                wait = exc.value + 1
                print(f"[MAP FLOOD] {key} wait={wait}s attempt={attempt + 1}", flush=True)
                await asyncio.sleep(wait)
        else:
            print(f"[MAP FAILED] {key} — retry next cycle", flush=True)
            return False

        discussion_chat_id = root.chat.id
        if discussion_chat_id not in COMMENT_GROUPS:
            print(f"[MAP IGNORED] source={key} discussion_chat={discussion_chat_id}", flush=True)
            recently_mapped[key] = now
            return True

        print(f"[MAP OK] source={source_chat_id}/{source_message_id} root={discussion_chat_id}/{root.id}", flush=True)
        recently_mapped[key] = now
        last_post_detected = time.monotonic()

        root_key = (discussion_chat_id, root.id)
        if root_key in comment_attempted or root_key in thread_watchers:
            return True
        waiting_roots[root_key] = time.monotonic() + WAIT_FOR_FIRST_COMMENT
        thread_watchers[root_key] = asyncio.create_task(
            watch_discussion_root(discussion_chat_id, root.id)
        )
        return True
    except FloodWait as exc:
        print(f"[MAP STILL FLOOD] {source_chat_id}/{source_message_id}: {exc!r}", flush=True)
        return False
    except Exception as exc:
        if "MSG_ID_INVALID" in repr(exc):
            # ارور دائمی: این پست قابل مپ نیست — دفعه بعد دوباره تلاش نکن
            print(f"[MAP SKIP PERMANENT] {source_chat_id}/{source_message_id}: {exc!r}", flush=True)
            recently_mapped[(source_chat_id, source_message_id)] = time.monotonic()
            return True
        print(f"[MAP ERROR] {source_chat_id}/{source_message_id}: {exc!r}", flush=True)
        return False


async def poll_group_forwarded_roots():
    """فالبک مپ: فوروارد چنل را مستقیم داخل گروه پیدا می‌کند (وقتی GetDiscussionMessage MSG_ID_INVALID دهد)."""
    while True:
        try:
            for chat_id in COMMENT_GROUPS:
                last = last_seen_group_msg.get(chat_id, 0)
                try:
                    async for item in app.get_chat_history(chat_id, limit=15):
                        if item.id <= last:
                            break
                        last_seen_group_msg[chat_id] = max(last_seen_group_msg.get(chat_id, 0), item.id)
                        fwd = getattr(item, "forward_from_chat", None)
                        if fwd is not None and fwd.id in SOURCE_CHANNELS:
                            root_key = (chat_id, item.id)
                            if root_key in comment_attempted or root_key in thread_watchers:
                                continue
                            print(f"[GROUP POLL ROOT] {chat_id}/{item.id} from channel={fwd.id}", flush=True)
                            waiting_roots[root_key] = time.monotonic() + WAIT_FOR_FIRST_COMMENT
                            thread_watchers[root_key] = asyncio.create_task(
                                watch_discussion_root(chat_id, item.id)
                            )
                except Exception as exc:
                    print(f"[GROUP POLL ERROR] {chat_id}: {exc!r}", flush=True)
        except Exception as exc:
            print(f"[GROUP POLL LOOP ERROR] {exc!r}", flush=True)
        await asyncio.sleep(POLL_INTERVAL + random.uniform(0, 3))


async def poll_source_channels():
    """هر POLL_INTERVAL ثانیه کانال را می‌پرسد و پست‌های جدید را پردازش می‌کند."""
    while True:
        try:
            for chat_id in SOURCE_CHANNELS:
                last = last_seen_channel_post.get(chat_id, 0)
                try:
                    async for item in app.get_chat_history(chat_id, limit=5):
                        if item.id <= last:
                            break
                        print(f"[NEW POST] {chat_id}/{item.id}", flush=True)
                        ok = await observe_channel_post_discussion(chat_id, item.id)
                        if not ok:
                            print(f"[DEFERRED] {chat_id}/{item.id} — retry next cycle", flush=True)
                            break
                        last_seen_channel_post[chat_id] = max(
                            last_seen_channel_post.get(chat_id, 0), item.id
                        )
                except Exception as exc:
                    print(f"[POLL CHANNEL ERROR] {chat_id}: {exc!r}", flush=True)
        except Exception as exc:
            print(f"[POLL LOOP ERROR] {exc!r}", flush=True)
        await asyncio.sleep(POLL_INTERVAL + random.uniform(0, 3))


@app.on_raw_update()
async def on_raw_update(client, update, users, chats):
    global last_post_detected
    if not isinstance(update, UpdateNewChannelMessage):
        return
    message = update.message
    if not isinstance(message, raw.types.Message):
        return
    peer = getattr(message, "peer_id", None)
    if not isinstance(peer, PeerChannel):
        return

    chat_id = chat_id_from_channel_id(peer.channel_id)
    message_id = message.id
    is_outgoing = bool(getattr(message, "out", False))
    text = (getattr(message, "message", "") or "")

    try:
        # ---- رصد پست‌های کانال (کامنت 🦦🦦) ----
        if chat_id in SOURCE_CHANNELS:
            if message_id > last_seen_channel_post.get(chat_id, 0):
                print(f"[RAW POST] {chat_id}/{message_id}", flush=True)
                last_seen_channel_post[chat_id] = max(last_seen_channel_post.get(chat_id, 0), message_id)
                last_post_detected = time.monotonic()
                asyncio.create_task(observe_channel_post_discussion(chat_id, message_id))

        # ---- خود-حذفی: اگر کسی با کلمه تریگر به پیام ما ریپلی کرد، پاک کن ----
        if chat_id not in DELETE_GROUPS:
            return

        from_id = getattr(message, "from_id", None)
        mine = is_outgoing or (
            isinstance(from_id, PeerUser)
            and MY_USER_ID is not None
            and from_id.user_id == MY_USER_ID
        )
        if mine:
            my_messages[chat_id].add(message_id)
            print(f"[MY MESSAGE] {chat_id}/{message_id}", flush=True)
            return

        # ---- مسیر سریع «کامنت دوم»: اولین کامنت بیرونی روی ریشهٔ منتظر → همین لحظه بفرست ----
        _rh = getattr(message, "reply_to", None)
        _rid = getattr(_rh, "reply_to_msg_id", None) if _rh else None
        _tid = getattr(_rh, "reply_to_top_id", None) if _rh else None
        _fast_root = _tid or _rid
        if _fast_root:
            _fk = (chat_id, _fast_root)
            if _fk in waiting_roots and _fk not in comment_attempted:
                print(f"[FAST PATH] external reply on {_fk} -> comment NOW", flush=True)
                await reserve_and_send(chat_id, _fast_root, "raw-external-reply")
                return

        reply_header = getattr(message, "reply_to", None)
        replied_id = getattr(reply_header, "reply_to_msg_id", None) if reply_header else None
        if not replied_id:
            return

        if not any(word.casefold() in text.casefold() for word in TRIGGER_WORDS):
            return
        if not await target_is_mine(chat_id, replied_id):
            print(f"[TRIGGER IGNORED: TARGET NOT MINE] {chat_id}/{replied_id}", flush=True)
            return

        print(f"[DELETE TRIGGER] {chat_id}/{replied_id} by reply={message_id}", flush=True)
        await delete_now(chat_id, replied_id)
    except Exception as exc:
        print(f"[RAW HANDLER ERROR] chat={chat_id} msg={message_id}: {exc!r}", flush=True)
        print(traceback.format_exc(), flush=True)


async def main():
    global MY_USER_ID
    async with app:
        me = await app.get_me()
        MY_USER_ID = me.id
        print(f"[BOT2 LOGGED IN] id={me.id} username={me.username}", flush=True)
        try:
            await app.send_message("me", "script: bot2.py\nacc: bot2")
            print("[SELF MSG SENT]", flush=True)
        except Exception as exc:
            print(f"[SELF MSG ERROR] {exc!r}", flush=True)

        for cid in SOURCE_CHANNELS:
            try:
                async for item in app.get_chat_history(cid, limit=1):
                    last_seen_channel_post[cid] = item.id
                    print(f"[BASELINE] channel={cid} top_id={item.id}", flush=True)
                    break
            except Exception as exc:
                print(f"[BASELINE ERROR] {cid}: {exc!r}", flush=True)

        for cid in COMMENT_GROUPS:
            try:
                async for item in app.get_chat_history(cid, limit=1):
                    last_seen_group_msg[cid] = item.id
                    print(f"[GROUP BASELINE] {cid} top_id={item.id}", flush=True)
                    break
            except Exception as exc:
                print(f"[GROUP BASELINE ERROR] {cid}: {exc!r}", flush=True)

        for cid in COMMENT_GROUPS | DELETE_GROUPS:
            try:
                await get_channel_peers(cid)
                print(f"[READY] group={cid}", flush=True)
            except Exception as exc:
                print(f"[PREWARM ERROR] {cid}: {exc!r}", flush=True)

        for cid in DELETE_GROUPS:
            await index_recent_own_messages(cid)

        asyncio.create_task(poll_source_channels())
        asyncio.create_task(poll_group_forwarded_roots())
        asyncio.create_task(profile_watchdog())
        print("[BOT2 STARTED]", flush=True)
        await asyncio.Event().wait()


app.run(main())
