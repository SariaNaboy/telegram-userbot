# -*- coding: utf-8 -*-
"""
نسخهٔ اصلاح‌شده‌ی یوزربات — تفاوت اصلی با نسخهٔ قبلی:

1) پولینگ فعال (مهم‌ترین اصلاح):
   تلگرام تضمین نمی‌کند آپدیت پوشِ پست‌های چنل را بفرستد (فقط "usually" می‌فرستد و
   ممکن است بی‌صدا قطع شود). برای همین کد قبلی هیچ لاگی نمی‌داد.
   این نسخه علاوه بر هندلرهای پوش، هر SOURCE_POLL_INTERVAL ثانیه چنل رادیو و
   گروه‌های دیسکاشن را خودش می‌پرسد و پست‌های جدید را پیدا می‌کند.

2) DEBUG_UPDATES=true -> همهٔ آپدیت‌های خام و همهٔ پیام‌ها لاگ می‌شوند.

3) تنظیمات از طریق env:
   SOURCE_POLL_INTERVAL   (پیش‌فرض 2.0)  فاصلهٔ پولینگ
   COMMENT_IF_NO_REPLIES  (پیش‌فرض false) اگر تا تایم‌اوت کامنتی نیامد باز هم کامنت بفرست
   ENABLE_STARTUP_RECOVERY (پیش‌فرض true) در استارت روی جدیدترین پستِ دارای کامنت کامنت بفرست
   DEBUG_UPDATES          (پیش‌فرض false)
"""
import os
import time
import asyncio
import secrets
import random
import traceback
import logging
from collections import defaultdict

# لاگ داخلی pyrogram (مثل UpdateChannelTooLong) هم دیده شود:
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s: %(message)s",
)

from io import BytesIO
from typing import List, Any

from pyrogram import Client, filters, idle, raw
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

# فقط چت‌هایی که با این سشن معتبرند (بقیه Peer id invalid می‌دادند)
DELETE_GROUPS = {
    -1001596320253,
}
COMMENT_GROUPS = {
    -1001596320253,
}

# Source channels whose post updates may arrive even when Telegram doesn't send
# the automatic-forward update from the linked discussion group.
DISCUSSION_SOURCE_CHANNELS = {
    -1001279727614,
}
TRIGGER_WORDS = {
    "گزارش", "report", "@admin", "صیک", "سیک",
    "اخطار", "بن", "سکوت", "ban", "mute",
}
# کامنت‌های متنوع (به‌جای یک متن تکراری — کاهش سیگنال اسپم)
# کامنت‌ها: (متن, وزن) — «حق» وزن بیشتر دارد و بیشتر انتخاب می‌شود
COMMENT_TEXTS = [
    "😑😑",
    "😐😐",
    "🤐🤐",
    "🫠🫠",
    "🫤🫤",
    "😕😕",
    "حق",
]
COMMENT_WEIGHTS = [
    1,  # 😑😑
    1,  # 😐😐
    1,  # 🤐🤐
    1,  # 🫠🫠
    1,  # 🫤🫤
    1,  # 😕😕
    3,  # حق — شانس ۳ برابر بقیه
]
# احتمال گذاشتن کامنت (۰.۷۵ = ۷۵٪)؛ ۲۵٪ مواقع عمداً کامنت نمی‌گذاریم
COMMENT_CHANCE = float(os.getenv("COMMENT_CHANCE", "0.85"))

# نوتیفیکیشن ادمین: با تأخیر رندوم ۲-۵ دقیقه و بدون لینک
ADMIN_NOTIFY_MIN_DELAY = int(os.getenv("ADMIN_NOTIFY_MIN_DELAY", "120"))   # 2 دقیقه
ADMIN_NOTIFY_MAX_DELAY = int(os.getenv("ADMIN_NOTIFY_MAX_DELAY", "300"))   # 5 دقیقه
ADMIN_NOTIFY_WORDS = [
    "شد", "ثبت", "انجام", "اوکی", "رفت",
    "تمام", "کامنت", "حله", "گرفت", "شد شد",
]
# سقف کامنت در هر ساعت (محافظ ضداسپم؛ با env قابل تغییر)
MAX_COMMENTS_PER_HOUR = int(os.getenv("MAX_COMMENTS_PER_HOUR", "6"))

comment_sent_times = []   # timestamps کامنت‌های ارسال‌شده
recently_mapped = {}    # (chat_id, msg_id) -> time.monotonic() — جلوگیری از get_discussion_message تکراری
my_comments = []         # لیست (chat_id, message_id) کامنت‌هایی که فرستادیم — با پست بعدی همه پاک می‌شوند
MAX_TRACKED_COMMENTS = 5  # حداکثر تعداد کامنت برای ردیابی (قدیمی‌ترها خارج می‌شوند)
WAIT_FOR_FIRST_COMMENT = int(os.getenv("WAIT_FOR_FIRST_COMMENT", "120"))
OWN_MESSAGE_HISTORY_LIMIT = 1000
COMMENT_RECOVERY_HISTORY_LIMIT = 1500

# ---- تنظیمات جدید ----
SOURCE_POLL_INTERVAL = float(os.getenv("SOURCE_POLL_INTERVAL", "15.0"))
COMMENT_IF_NO_REPLIES = os.getenv("COMMENT_IF_NO_REPLIES", "false").lower() in {"1", "true", "yes"}
ENABLE_STARTUP_RECOVERY = os.getenv("ENABLE_STARTUP_RECOVERY", "true").lower() in {"1", "true", "yes"}
DEBUG_UPDATES = os.getenv("DEBUG_UPDATES", "false").lower() in {"1", "true", "yes"}

# ===== تغییر هویت خودکار =====
# pyrogram 2.0.106 تایپ inputPrivacyKeyAbout را ندارد؛ خودمان با ID درست می‌سازیم
class InputPrivacyKeyAbout(TLObject):
    """inputPrivacyKeyAbout#b66b4d6a = InputPrivacyKey;"""
    __slots__: List[str] = []

    # ID درست طبق scheme رسمی تلگرام: inputPrivacyKeyAbout#3823cc40
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


PROFILE_AMIRALI_NAME = "𝑩𝒍𝒂𝒄𝒌 𝑳𝒖𝒏𝒈 𝑴𝒐𝒓𝒈𝒂𝒏"
PROFILE_AMIRALI_USERNAME = "Amirali126868"
PROFILE_MAYA_NAME = "Maya"
PROFILE_MAYA_USERNAME = ""          # بدون یوزرنیم
PROFILE_REVERT_SECONDS = 1800       # ۳۰ دقیقه بعد از آخرین پست بدون پست جدید

last_post_detected = None           # time.monotonic() آخرین باری که پست جدید دیدیم
profile_mode = "maya"               # "maya" یا "amirali"

app = Client(
    "userbot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION,
    sleep_threshold=0,
    workers=32,
)

MY_USER_ID = None
peer_cache = {}
my_messages = defaultdict(set)
# root_key = (discussion_chat_id, forwarded_root_message_id)
waiting_roots = {}
comment_attempted = set()
comment_sent = set()   # ریشه‌هایی که کامنتشان واقعاً فرستاده شد — برای توقف واچر
thread_watchers = {}
recovery_checked = set()
logged_reply_roots = set()

# خط مبنای پولینگ: بالاترین message id که قبلاً دیده‌ایم
last_seen_channel_post = {}   # chat_id -> highest post id
last_seen_group_message = {}  # chat_id -> highest group message id


def chat_id_from_channel_id(channel_id: int) -> int:
    return -(1_000_000_000_000 + channel_id)


async def notify_admin(text: str):
    if not ADMIN_USERNAME:
        return
    try:
        await app.send_message(ADMIN_USERNAME, text)
        print("[ADMIN NOTIFIED]", flush=True)
    except Exception as exc:
        print(f"[ADMIN NOTIFY ERROR] {exc!r}", flush=True)


async def notify_admin_delayed():
    """بعد از تأخیر رندوم ۲-۵ دقیقه، فقط یک کلمه به ادمین می‌فرستد (بدون لینک)."""
    try:
        delay = random.randint(ADMIN_NOTIFY_MIN_DELAY, ADMIN_NOTIFY_MAX_DELAY)
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
    return await app.invoke(
        raw.functions.account.SetPrivacy(key=key, rules=[rule])
    )


async def apply_profile_amirali():
    """هویت AmirAli: نام + مخفی‌کردن عکس و بیو از همه."""
    global profile_mode
    try:
        await app.update_profile(first_name=PROFILE_AMIRALI_NAME)
        await set_privacy_rule(raw.types.InputPrivacyKeyProfilePhoto(), allow_all=False)
        await set_privacy_rule(InputPrivacyKeyAbout(), allow_all=False)
        profile_mode = "amirali"
        print(
            "[PROFILE -> AmirAli] name=AmirAli photo=hidden bio=hidden",
            flush=True,
        )
    except Exception as exc:
        print(f"[PROFILE AMIRALI ERROR] {exc!r}", flush=True)
        print(traceback.format_exc(), flush=True)


async def apply_profile_maya():
    """هویت Maya: نام + نمایش عکس و بیو برای همه."""
    global profile_mode
    try:
        await app.update_profile(first_name=PROFILE_MAYA_NAME)
        await set_privacy_rule(raw.types.InputPrivacyKeyProfilePhoto(), allow_all=True)
        await set_privacy_rule(InputPrivacyKeyAbout(), allow_all=True)
        profile_mode = "maya"
        print(
            "[PROFILE -> Maya] name=Maya photo=public bio=public",
            flush=True,
        )
    except Exception as exc:
        print(f"[PROFILE MAYA ERROR] {exc!r}", flush=True)
        print(traceback.format_exc(), flush=True)


async def profile_watchdog():
    """اگر ۳۰ دقیقه از آخرین پست گذشت و پست جدیدی نیامد، به Maya برمی‌گردد."""
    global profile_mode
    while True:
        try:
            if (
                profile_mode == "amirali"
                and last_post_detected is not None
                and time.monotonic() - last_post_detected >= PROFILE_REVERT_SECONDS
            ):
                print("[PROFILE TIMER] ۳۰ دقیقه بدون پست جدید -> بازگشت به Maya", flush=True)
                await apply_profile_maya()
        except Exception as exc:
            print(f"[PROFILE WATCHDOG ERROR] {exc!r}", flush=True)
        await asyncio.sleep(10)


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
    indexed = 0
    try:
        async for item in app.get_chat_history(chat_id, limit=OWN_MESSAGE_HISTORY_LIMIT):
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


def extract_sent_msg_id(result):
    """message id پیامی که تازه فرستادیم را از پاسخ raw استخراج می‌کند."""
    for u in getattr(result, "updates", []) or []:
        if isinstance(u, raw.types.UpdateMessageID):
            return u.id
        if isinstance(u, raw.types.UpdateNewChannelMessage):
            m = u.message
            if isinstance(m, raw.types.Message):
                return m.id
    return None


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


async def send_comment_after_external_reply(chat_id: int, root_message_id: int):
    """Send one reply to a discussion root. The caller must reserve the root first."""
    global last_post_detected, my_comments
    try:
        peer, _ = await get_channel_peers(chat_id)
        print(
            f"[COMMENT SEND ATTEMPT] chat={chat_id} root={root_message_id} "
            f"peer_channel_id={peer.channel_id}",
            flush=True,
        )
        result = await app.invoke(
            raw.functions.messages.SendMessage(
                peer=peer,
                message=random.choices(COMMENT_TEXTS, weights=COMMENT_WEIGHTS, k=1)[0],
                random_id=secrets.randbits(63),
                reply_to_msg_id=root_message_id,
                no_webpage=True,
            )
        )
        print(
            f"[COMMENT SENT] chat={chat_id} root={root_message_id} "
            f"updates_type={type(result).__name__}",
            flush=True,
        )
        sent_id = extract_sent_msg_id(result)
        if sent_id:
            my_comments.append((chat_id, sent_id))
            # فقط آخرین MAX_TRACKED_COMMENTS را نگه می‌داریم
            del my_comments[:-MAX_TRACKED_COMMENTS]
            print(f"[COMMENT TRACKED] chat={chat_id} msg={sent_id} total={len(my_comments)}", flush=True)
        asyncio.create_task(notify_admin_delayed())

        # تغییر هویت به AmirAli + ریست تایمر «آخرین پست»
        last_post_detected = time.monotonic()
        if profile_mode != "amirali":
            asyncio.create_task(apply_profile_amirali())
        else:
            print("[PROFILE] already AmirAli — timer reset", flush=True)
    except FloodWait as exc:
        print(f"[COMMENT SKIPPED: FLOOD {exc.value}s] {chat_id}/{root_message_id}", flush=True)
    except Exception as exc:
        print(f"[COMMENT ERROR] {chat_id}/{root_message_id}: {exc!r}", flush=True)
        print(traceback.format_exc(), flush=True)


def comments_in_last_hour() -> int:
    now = time.monotonic()
    while comment_sent_times and now - comment_sent_times[0] > 3600:
        comment_sent_times.pop(0)
    return len(comment_sent_times)


async def send_comment_direct(chat_id: int, root_message_id: int, reason: str):
    """ارسال مستقیم کامنت — تصمیم (شانس/سقف ساعتی) قبلاً در observe_discussion_root گرفته شده."""
    comment_sent.add((chat_id, root_message_id))
    waiting_roots.pop((chat_id, root_message_id), None)
    comment_sent_times.append(time.monotonic())
    print(f"[COMMENT SEND GO] {(chat_id, root_message_id)} reason={reason}", flush=True)
    await send_comment_after_external_reply(chat_id, root_message_id)
    return True


async def watch_discussion_root(chat_id: int, root_message_id: int):
    """Wait at most WAIT_FOR_FIRST_COMMENT seconds for the first external reply."""
    root_key = (chat_id, root_message_id)
    deadline = time.monotonic() + WAIT_FOR_FIRST_COMMENT
    last_count = None
    invalid_hits = 0

    try:
        if not hasattr(app, "get_discussion_replies_count"):
            print(
                "[ERROR] Pyrogram >= 2.0 needed for get_discussion_replies_count "
                "— run: pip install -U pyrogram",
                flush=True,
            )
            return

        while time.monotonic() < deadline:
            if root_key in comment_sent:
                return
            try:
                count = await app.get_discussion_replies_count(chat_id, root_message_id)
                if count != last_count:
                    print(f"[WATCHER COUNT] {root_key} count={count}", flush=True)
                    last_count = count
                if count >= 1:
                    await send_comment_direct(chat_id, root_message_id, "watcher-count")
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
                        return
                    await asyncio.sleep(4)
                    continue
                invalid_hits = 0
                if "FLOOD_WAIT" in repr(exc):
                    # وقتی flood آمد، همین مقدار صبر کن تا تلگرام آرام بگیرد
                    await asyncio.sleep(5)
            await asyncio.sleep(4)

        waiting_roots.pop(root_key, None)
        print(f"[WATCHER TIMEOUT] {root_key}", flush=True)

        # اگر هیچ کامنتی نیامد ولی خواسته شده باز هم کامنت فرستاده شود:
        if COMMENT_IF_NO_REPLIES:
            await send_comment_direct(chat_id, root_message_id, "timeout-force")
    finally:
        thread_watchers.pop(root_key, None)


async def observe_channel_post_discussion(source_chat_id: int, source_message_id: int):
    """Map a source-channel post to its linked discussion-group root.

    Returns True on success (post fully handled), False if it should be retried
    later (e.g. FloodWait) so the poll loop doesn't mark it as seen.
    """
    global last_post_detected
    try:
        if not hasattr(app, "get_discussion_message"):
            print(
                "[ERROR] Pyrogram >= 2.0 needed for get_discussion_message "
                "— run: pip install -U pyrogram",
                flush=True,
            )
            return False

        # ددپلیکیت: اگر همین پست را قبلاً در این فرآیند map کرده‌ایم، دوباره RPC نزن
        key = (source_chat_id, source_message_id)
        now = time.monotonic()
        if key in recently_mapped and now - recently_mapped[key] < 120:
            return True

        try:
            discussion_root = await app.get_discussion_message(
                source_chat_id,
                source_message_id,
            )
        except FloodWait as exc:
            wait = exc.value + 1
            print(
                f"[DISCUSSION MAP FLOOD] source={source_chat_id}/{source_message_id} "
                f"wait={wait}s — retrying",
                flush=True,
            )
            await asyncio.sleep(wait)
            discussion_root = await app.get_discussion_message(
                source_chat_id,
                source_message_id,
            )

        discussion_chat_id = discussion_root.chat.id
        if discussion_chat_id not in COMMENT_GROUPS:
            print(
                f"[DISCUSSION MAP IGNORED] source={source_chat_id}/{source_message_id} "
                f"discussion_chat={discussion_chat_id}",
                flush=True,
            )
            recently_mapped[key] = now
            return True

        print(
            f"[DISCUSSION MAP] source={source_chat_id}/{source_message_id} "
            f"root={discussion_chat_id}/{discussion_root.id}",
            flush=True,
        )
        recently_mapped[key] = now
        last_post_detected = time.monotonic()
        await observe_discussion_root(
            discussion_chat_id,
            discussion_root.id,
            source_channel_id=source_chat_id,
            known_count=None,
            source="channel-post-map",
        )
        return True
    except FloodWait as exc:
        # حتی بعد از retry flood ماند → بگذار چرخهٔ بعد دوباره تلاش کند
        print(
            f"[DISCUSSION MAP STILL FLOOD] source={source_chat_id}/{source_message_id}: {exc!r}",
            flush=True,
        )
        return False
    except Exception as exc:
        if "MSG_ID_INVALID" in repr(exc):
            # ارور دائمی: این پست قابل مپ نیست — دفعه بعد دوباره تلاش نکن
            print(
                f"[DISCUSSION MAP SKIP PERMANENT] source={source_chat_id}/{source_message_id}: {exc!r}",
                flush=True,
            )
            recently_mapped[(source_chat_id, source_message_id)] = time.monotonic()
            return True
        print(
            f"[DISCUSSION MAP ERROR] source={source_chat_id}/{source_message_id}: {exc!r}",
            flush=True,
        )
        return False


async def observe_discussion_root(chat_id: int, root_message_id: int, source_channel_id=None, known_count=None, source="unknown"):
    """Common root entry point used by high-level, raw and polling handlers."""
    """ترتیب جدید (درخواست کاربر):
    ۱) تصمیم کامنت اولِ کار گرفته می‌شود (شانس + سقف ساعتی)
    ۲) اگر بله: هویت *قبل* از کامنت فوراً به AmirAli می‌رود
    ۳) الگوریتم کامنت شروع می‌شود — هدف: کامنت دوم بودن
    ۴) در انتها (چه کامنت بگذارد چه نه) کامنت قبلی پاک می‌شود
    """
    global last_post_detected, my_comments
    root_key = (chat_id, root_message_id)
    if root_key in comment_attempted or root_key in thread_watchers:
        return

    last_post_detected = time.monotonic()

    print(
        f"[ROOT DETECTED] chat={chat_id} root={root_message_id} "
        f"source_channel={source_channel_id} known_count={known_count} via={source}",
        flush=True,
    )

    # ---- ۱) تصمیم فوری ----
    decided_comment = True
    if random.random() > COMMENT_CHANCE:
        decided_comment = False
        print(f"[COMMENT DECIDED NO (chance)] {root_key} chance={COMMENT_CHANCE}", flush=True)
    elif comments_in_last_hour() >= MAX_COMMENTS_PER_HOUR:
        decided_comment = False
        print(f"[COMMENT DECIDED NO (rate-limit)] {root_key} already {MAX_COMMENTS_PER_HOUR}/hour", flush=True)

    # ریشه از این لحظه «تصمیم‌گرفته‌شده» است؛ مسیرهای دیگر دیگر roll نمی‌زنند
    comment_attempted.add(root_key)
    waiting_roots.pop(root_key, None)

    if decided_comment:
        print(f"[COMMENT DECIDED YES] {root_key}", flush=True)
        # ---- ۲) هویت فوراً عوض شود (قبل از هر کامنت) ----
        asyncio.create_task(apply_profile_amirali())
        # ---- ۳) الگوریتم کامنت ----
        if known_count is not None and known_count >= 1:
            # پست را دیر دیده‌ایم و از قبل کامنت دارد → همان لحظه بفرست (می‌شود دومیِ بعدی)
            await send_comment_direct(chat_id, root_message_id, "root-already-has-replies")
        else:
            # صبر برای اولین کامنت بیرونی → بعد بفرست تا «دوم» شود
            waiting_roots[root_key] = time.monotonic() + WAIT_FOR_FIRST_COMMENT
            thread_watchers[root_key] = asyncio.create_task(
                watch_discussion_root(chat_id, root_message_id)
            )

    # ---- ۴) پاک‌سازی کامنت‌های قبلی — همیشه، ولی در انتها تا سرعت کامنت‌گذاری حفظ شود ----
    if my_comments:
        to_delete = list(my_comments)
        my_comments.clear()
        for old_chat, old_msg in to_delete:
            print(f"[DELETE PREV COMMENT] {old_chat}/{old_msg}", flush=True)
            asyncio.create_task(delete_now(old_chat, old_msg))


@app.on_message(filters.chat(list(DISCUSSION_SOURCE_CHANNELS)) & ~filters.outgoing)
async def detect_source_channel_post_high_level(client, message):
    """Primary path: channel post update -> Telegram discussion-root mapping."""
    try:
        await observe_channel_post_discussion(message.chat.id, message.id)
    except Exception as exc:
        print(f"[SOURCE POST HANDLER ERROR] {exc!r}", flush=True)
        print(traceback.format_exc(), flush=True)


@app.on_message(filters.chat(list(COMMENT_GROUPS)) & ~filters.outgoing)
async def detect_discussion_root_high_level(client, message):
    try:
        forwarded_from = getattr(message, "forward_from_chat", None)
        if not forwarded_from:
            return

        replies_obj = getattr(message, "replies", None)
        reply_count = getattr(replies_obj, "replies", None) if replies_obj else None
        await observe_discussion_root(
            message.chat.id,
            message.id,
            source_channel_id=getattr(forwarded_from, "id", None),
            known_count=reply_count,
            source="high-level",
        )
    except Exception as exc:
        print(f"[HIGH LEVEL ROOT ERROR] {exc!r}", flush=True)
        print(traceback.format_exc(), flush=True)


@app.on_message(filters.chat(list(DELETE_GROUPS)) & filters.outgoing)
async def remember_outgoing_message(client, message):
    my_messages[message.chat.id].add(message.id)
    print(f"[MY MESSAGE HIGH LEVEL] {message.chat.id}/{message.id}", flush=True)


# فقط برای دیباگ: همهٔ پیام‌ها را نشان می‌دهد (با DEBUG_UPDATES=true)
if DEBUG_UPDATES:
    @app.on_message()
    async def debug_any_message(client, message):
        print(
            f"[DEBUG MSG] chat={message.chat.id} id={message.id} out={message.outgoing} "
            f"text={(message.text or '')[:60]!r}",
            flush=True,
        )


@app.on_raw_update()
async def on_raw_update(client, update, users, chats):
    if DEBUG_UPDATES:
        print(f"[RAW UPDATE] {type(update).__name__}", flush=True)

    if isinstance(update, raw.types.UpdateChannelTooLong):
        # چنل بزرگ/پرمشترک: تلگرام به‌جای پوش هر پست، این را می‌فرستد.
        # خود pyrogram فقط در سطح INFO لاگ می‌زند؛ ما اینجا واضح هشدار می‌دهیم.
        # پولینگ poll_new_channel_posts این مورد را جبران می‌کند.
        print(
            f"[CHANNEL TOO LONG] channel={getattr(update, 'channel_id', None)} "
            f"pts={getattr(update, 'pts', None)} -> پوش نمی‌رسد، پولینگ فعال است",
            flush=True,
        )
        return

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

    try:
        # A source-channel post and its auto-forward in the linked discussion
        # group have different message IDs. Map it with Telegram's documented
        # getDiscussionMessage method instead of guessing an ID relationship.
        if chat_id in DISCUSSION_SOURCE_CHANNELS and getattr(message, "post", False):
            await observe_channel_post_discussion(chat_id, message_id)
            return

        # Raw fallback: useful if a high-level update is delivered out of order.
        if chat_id in COMMENT_GROUPS:
            fwd_from = getattr(getattr(message, "fwd_from", None), "from_id", None)
            if isinstance(fwd_from, PeerChannel):
                replies_obj = getattr(message, "replies", None)
                reply_count = getattr(replies_obj, "replies", None) if replies_obj else None
                await observe_discussion_root(
                    chat_id, message_id, fwd_from.channel_id, reply_count, "raw"
                )
                return

            if not is_outgoing:
                reply_header = getattr(message, "reply_to", None)
                reply_id = getattr(reply_header, "reply_to_msg_id", None) if reply_header else None
                top_id = getattr(reply_header, "reply_to_top_id", None) if reply_header else None
                candidate_root_id = top_id or reply_id

                if candidate_root_id:
                    sample_key = (chat_id, candidate_root_id)
                    if sample_key not in logged_reply_roots:
                        logged_reply_roots.add(sample_key)
                        print(
                            f"[COMMENT REPLY SAMPLE] chat={chat_id} msg={message_id} "
                            f"reply_to={reply_id} top={top_id}",
                            flush=True,
                        )

                    if sample_key in waiting_roots and sample_key not in comment_sent:
                        await send_comment_direct(chat_id, candidate_root_id, "raw-external-reply")
                    elif sample_key not in recovery_checked:
                        recovery_checked.add(sample_key)
                        # Reply may arrive before its root update. Confirm that the
                        # referenced message really is a forwarded discussion root.
                        root = await app.get_messages(chat_id, candidate_root_id)
                        if root and getattr(root, "forward_from_chat", None):
                            replies = getattr(root, "replies", None)
                            count = getattr(replies, "replies", None) if replies else None
                            await observe_discussion_root(
                                chat_id,
                                candidate_root_id,
                                getattr(root.forward_from_chat, "id", None),
                                count,
                                "reply-recovery",
                            )

        # Delete logic.
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

        reply_header = getattr(message, "reply_to", None)
        replied_id = getattr(reply_header, "reply_to_msg_id", None) if reply_header else None
        if not replied_id:
            return

        text = (getattr(message, "message", "") or "").casefold()
        if not any(word.casefold() in text for word in TRIGGER_WORDS):
            return
        if not await target_is_mine(chat_id, replied_id):
            print(f"[TRIGGER IGNORED: TARGET NOT MINE] {chat_id}/{replied_id}", flush=True)
            return

        print(f"[DELETE TRIGGER] {chat_id}/{replied_id} by reply={message_id}", flush=True)
        await delete_now(chat_id, replied_id)

    except Exception as exc:
        # Never let one malformed/raw update silently kill the diagnostic path.
        print(f"[RAW HANDLER ERROR] chat={chat_id} msg={message_id}: {exc!r}", flush=True)
        print(traceback.format_exc(), flush=True)


# ================= پولینگ فعال =================

async def poll_new_channel_posts():
    """هر SOURCE_POLL_INTERVAL ثانیه چنل رادیو را می‌پرسد و پست‌های جدید را پیدا می‌کند."""
    while True:
        try:
            for chat_id in DISCUSSION_SOURCE_CHANNELS:
                last = last_seen_channel_post.get(chat_id, 0)
                try:
                    async for item in app.get_chat_history(chat_id, limit=5):
                        if item.id <= last:
                            break
                        print(f"[POLL NEW POST] {chat_id}/{item.id}", flush=True)
                        ok = await observe_channel_post_discussion(chat_id, item.id)
                        if not ok:
                            # پردازش fail شد؛ last_seen را عوض نکن تا چرخهٔ بعد دوباره تلاش کند
                            print(f"[POLL POST DEFERRED] {chat_id}/{item.id} — retry next cycle", flush=True)
                            break
                        last_seen_channel_post[chat_id] = max(
                            last_seen_channel_post.get(chat_id, 0), item.id
                        )
                except Exception as exc:
                    print(f"[POLL CHANNEL ERROR] {chat_id}: {exc!r}", flush=True)
        except Exception as exc:
            print(f"[POLL LOOP ERROR] {exc!r}", flush=True)
        await asyncio.sleep(SOURCE_POLL_INTERVAL + random.uniform(0, 3))


async def poll_new_discussion_roots():
    """گروه‌های دیسکاشن را می‌پرسد؛ ریشهٔ auto-forward جدید (پست چنل) را پیدا می‌کند."""
    while True:
        try:
            for chat_id in COMMENT_GROUPS:
                last = last_seen_group_message.get(chat_id, 0)
                try:
                    async for item in app.get_chat_history(chat_id, limit=10):
                        if item.id <= last:
                            break
                        last_seen_group_message[chat_id] = max(
                            last_seen_group_message.get(chat_id, 0), item.id
                        )
                        fwd = getattr(item, "forward_from_chat", None)
                        if fwd is not None and getattr(fwd, "id", None) in DISCUSSION_SOURCE_CHANNELS \
                                and getattr(item, "replies", None) is not None:
                            print(f"[POLL GROUP ROOT] {chat_id}/{item.id} fwd={fwd.id}", flush=True)
                            replies_obj = getattr(item, "replies", None)
                            reply_count = getattr(replies_obj, "replies", None) if replies_obj else None
                            await observe_discussion_root(
                                chat_id, item.id, fwd.id, reply_count, "group-poll"
                            )
                except Exception as exc:
                    print(f"[POLL GROUP ERROR] {chat_id}: {exc!r}", flush=True)
        except Exception as exc:
            print(f"[POLL LOOP ERROR] {exc!r}", flush=True)
        await asyncio.sleep(SOURCE_POLL_INTERVAL + random.uniform(0, 3))


async def find_recent_active_discussion_root(chat_id: int):
    """Find the newest forwarded discussion root that already has a reply."""
    peer, _ = await get_channel_peers(chat_id)
    offset_id = 0
    scanned = 0

    while scanned < COMMENT_RECOVERY_HISTORY_LIMIT:
        result = await app.invoke(
            raw.functions.messages.GetHistory(
                peer=peer,
                offset_id=offset_id,
                offset_date=0,
                add_offset=0,
                limit=min(100, COMMENT_RECOVERY_HISTORY_LIMIT - scanned),
                max_id=0,
                min_id=0,
                hash=0,
            )
        )
        messages = getattr(result, "messages", [])
        if not messages:
            return None

        for item in messages:
            if not isinstance(item, raw.types.Message):
                continue
            fwd_from = getattr(getattr(item, "fwd_from", None), "from_id", None)
            replies = getattr(getattr(item, "replies", None), "replies", 0)
            if isinstance(fwd_from, PeerChannel) and replies >= 1:
                return item.id, fwd_from.channel_id, replies

        ids = [item.id for item in messages if hasattr(item, "id")]
        if not ids:
            return None
        offset_id = min(ids)
        scanned += len(messages)
    return None


async def recover_recent_discussion_root(chat_id: int):
    try:
        found = await find_recent_active_discussion_root(chat_id)
        if found is None:
            print(f"[NO RECENT ACTIVE ROOT] chat={chat_id}", flush=True)
            return
        root_id, source_channel_id, count = found
        print(f"[STARTUP ROOT RECOVERY] chat={chat_id} root={root_id} count={count}", flush=True)
        await observe_discussion_root(
            chat_id, root_id, source_channel_id, count, "startup-recovery"
        )
    except Exception as exc:
        print(f"[STARTUP ROOT RECOVERY ERROR] chat={chat_id}: {exc!r}", flush=True)
        print(traceback.format_exc(), flush=True)


async def recover_my_recent_comments(chat_id: int):
    """در استارت، آخرین کامنت‌های خودمان را در گروه پیدا می‌کند تا با پست بعدی پاک شوند."""
    global my_comments
    try:
        found = 0
        async for item in app.get_chat_history(chat_id, limit=200):
            is_mine = bool(getattr(item, "outgoing", False)) or (
                getattr(item, "from_user", None) is not None
                and item.from_user.id == MY_USER_ID
            )
            if is_mine and item.reply_to_message_id is not None:
                my_comments.append((chat_id, item.id))
                found += 1
                if found >= MAX_TRACKED_COMMENTS:
                    break
        if found:
            print(f"[RECOVERED MY COMMENTS] chat={chat_id} count={found}", flush=True)
        else:
            print(f"[NO MY COMMENTS FOUND] chat={chat_id}", flush=True)
    except Exception as exc:
        print(f"[RECOVER MY COMMENTS ERROR] {chat_id}: {exc!r}", flush=True)
        print(traceback.format_exc(), flush=True)


async def main():
    global MY_USER_ID
    async with app:
        me = await app.get_me()
        MY_USER_ID = me.id
        print(f"[LOGGED IN] id={MY_USER_ID}", flush=True)
        try:
            await app.send_message("me", "script: main.py\nacc: main")
            print("[SELF MSG SENT]", flush=True)
        except Exception as exc:
            print(f"[SELF MSG ERROR] {exc!r}", flush=True)

        for chat_id in DELETE_GROUPS | COMMENT_GROUPS:
            try:
                await get_channel_peers(chat_id)
                print(f"[READY] {chat_id}", flush=True)
            except Exception as exc:
                print(f"[PREWARM ERROR] {chat_id}: {exc!r}", flush=True)

        # ---- ثبت خط مبنای پولینگ (تا پست‌های قدیمی دوباره پردازش نشوند) ----
        for chat_id in DISCUSSION_SOURCE_CHANNELS:
            try:
                async for item in app.get_chat_history(chat_id, limit=1):
                    last_seen_channel_post[chat_id] = item.id
                    print(f"[CHANNEL BASELINE] {chat_id} top_id={item.id}", flush=True)
                    break
            except Exception as exc:
                print(f"[CHANNEL BASELINE ERROR] {chat_id}: {exc!r}", flush=True)

        for chat_id in COMMENT_GROUPS:
            try:
                async for item in app.get_chat_history(chat_id, limit=1):
                    last_seen_group_message[chat_id] = item.id
                    print(f"[GROUP BASELINE] {chat_id} top_id={item.id}", flush=True)
                    break
            except Exception as exc:
                print(f"[GROUP BASELINE ERROR] {chat_id}: {exc!r}", flush=True)

        # ---- شروع پولینگ فعال ----
        asyncio.create_task(poll_new_channel_posts())
        asyncio.create_task(poll_new_discussion_roots())
        asyncio.create_task(profile_watchdog())

        if ENABLE_STARTUP_RECOVERY:
            for chat_id in COMMENT_GROUPS:
                await recover_recent_discussion_root(chat_id)

        # بازیابی کامنت‌های قبلی خودمان (تا با اولین پست جدید پاک شوند)
        for chat_id in COMMENT_GROUPS:
            await recover_my_recent_comments(chat_id)

        for chat_id in DELETE_GROUPS:
            await index_recent_own_messages(chat_id)

        print("[STARTED]", flush=True)
        await idle()


app.run(main())
