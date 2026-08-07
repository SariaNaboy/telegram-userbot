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
import traceback
import logging
from collections import defaultdict

# لاگ داخلی pyrogram (مثل UpdateChannelTooLong) هم دیده شود:
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(name)s: %(message)s",
)

from pyrogram import Client, filters, idle, raw
from pyrogram.errors import FloodWait
from pyrogram.raw.types import (
    PeerUser,
    PeerChannel,
    InputPeerChannel,
    InputChannel,
    UpdateNewChannelMessage,
)

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
COMMENT_TEXT = "🤔🤔🤔🤔"
WAIT_FOR_FIRST_COMMENT = int(os.getenv("WAIT_FOR_FIRST_COMMENT", "120"))
OWN_MESSAGE_HISTORY_LIMIT = 1000
COMMENT_RECOVERY_HISTORY_LIMIT = 1500

# ---- تنظیمات جدید ----
SOURCE_POLL_INTERVAL = float(os.getenv("SOURCE_POLL_INTERVAL", "2.0"))
COMMENT_IF_NO_REPLIES = os.getenv("COMMENT_IF_NO_REPLIES", "false").lower() in {"1", "true", "yes"}
ENABLE_STARTUP_RECOVERY = os.getenv("ENABLE_STARTUP_RECOVERY", "true").lower() in {"1", "true", "yes"}
DEBUG_UPDATES = os.getenv("DEBUG_UPDATES", "false").lower() in {"1", "true", "yes"}

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
                message=COMMENT_TEXT,
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
        internal_id = str(chat_id)[4:] if str(chat_id).startswith("-100") else str(abs(chat_id))
        asyncio.create_task(notify_admin(
            "کامنت ثبت شد:\n"
            f"https://t.me/c/{internal_id}/{root_message_id}"
        ))
    except FloodWait as exc:
        print(f"[COMMENT SKIPPED: FLOOD {exc.value}s] {chat_id}/{root_message_id}", flush=True)
    except Exception as exc:
        print(f"[COMMENT ERROR] {chat_id}/{root_message_id}: {exc!r}", flush=True)
        print(traceback.format_exc(), flush=True)


async def reserve_and_send_comment(chat_id: int, root_message_id: int, reason: str):
    """Atomically reserve a root in this process, then send exactly once."""
    root_key = (chat_id, root_message_id)
    if root_key in comment_attempted:
        return False

    comment_attempted.add(root_key)
    waiting_roots.pop(root_key, None)
    print(f"[COMMENT RESERVED] {root_key} reason={reason}", flush=True)
    await send_comment_after_external_reply(chat_id, root_message_id)
    return True


async def watch_discussion_root(chat_id: int, root_message_id: int):
    """Wait at most WAIT_FOR_FIRST_COMMENT seconds for the first external reply."""
    root_key = (chat_id, root_message_id)
    deadline = time.monotonic() + WAIT_FOR_FIRST_COMMENT
    last_count = None

    try:
        if not hasattr(app, "get_discussion_replies_count"):
            print(
                "[ERROR] Pyrogram >= 2.0 needed for get_discussion_replies_count "
                "— run: pip install -U pyrogram",
                flush=True,
            )
            return

        while time.monotonic() < deadline:
            if root_key in comment_attempted:
                return
            try:
                count = await app.get_discussion_replies_count(chat_id, root_message_id)
                if count != last_count:
                    print(f"[WATCHER COUNT] {root_key} count={count}", flush=True)
                    last_count = count
                if count >= 1:
                    await reserve_and_send_comment(chat_id, root_message_id, "watcher-count")
                    return
            except Exception as exc:
                print(f"[WATCHER COUNT ERROR] {root_key}: {exc!r}", flush=True)
                if "MSG_ID_INVALID" in repr(exc):
                    waiting_roots.pop(root_key, None)
                    return
            await asyncio.sleep(1)

        waiting_roots.pop(root_key, None)
        print(f"[WATCHER TIMEOUT] {root_key}", flush=True)

        # اگر هیچ کامنتی نیامد ولی کاربر خواسته باز هم کامنت فرستاده شود:
        if COMMENT_IF_NO_REPLIES and root_key not in comment_attempted:
            await reserve_and_send_comment(chat_id, root_message_id, "timeout-force")
    finally:
        thread_watchers.pop(root_key, None)


async def observe_channel_post_discussion(source_chat_id: int, source_message_id: int):
    """Map a source-channel post to its linked discussion-group root."""
    try:
        if not hasattr(app, "get_discussion_message"):
            print(
                "[ERROR] Pyrogram >= 2.0 needed for get_discussion_message "
                "— run: pip install -U pyrogram",
                flush=True,
            )
            return

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
            return

        print(
            f"[DISCUSSION MAP] source={source_chat_id}/{source_message_id} "
            f"root={discussion_chat_id}/{discussion_root.id}",
            flush=True,
        )
        await observe_discussion_root(
            discussion_chat_id,
            discussion_root.id,
            source_channel_id=source_chat_id,
            known_count=None,
            source="channel-post-map",
        )
    except Exception as exc:
        print(
            f"[DISCUSSION MAP ERROR] source={source_chat_id}/{source_message_id}: {exc!r}",
            flush=True,
        )
        print(traceback.format_exc(), flush=True)


async def observe_discussion_root(chat_id: int, root_message_id: int, source_channel_id=None, known_count=None, source="unknown"):
    """Common root entry point used by high-level, raw and polling handlers."""
    root_key = (chat_id, root_message_id)
    if root_key in comment_attempted or root_key in thread_watchers:
        return

    print(
        f"[ROOT DETECTED] chat={chat_id} root={root_message_id} "
        f"source_channel={source_channel_id} known_count={known_count} via={source}",
        flush=True,
    )

    # A root seen late may already have comments. Do not wait for an update that
    # happened before this runner connected.
    if known_count is not None and known_count >= 1:
        await reserve_and_send_comment(chat_id, root_message_id, "root-already-has-replies")
        return

    waiting_roots[root_key] = time.monotonic() + WAIT_FOR_FIRST_COMMENT
    thread_watchers[root_key] = asyncio.create_task(
        watch_discussion_root(chat_id, root_message_id)
    )


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

                    if sample_key in waiting_roots:
                        await reserve_and_send_comment(chat_id, candidate_root_id, "raw-external-reply")
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
                        last_seen_channel_post[chat_id] = max(
                            last_seen_channel_post.get(chat_id, 0), item.id
                        )
                        await observe_channel_post_discussion(chat_id, item.id)
                except Exception as exc:
                    print(f"[POLL CHANNEL ERROR] {chat_id}: {exc!r}", flush=True)
        except Exception as exc:
            print(f"[POLL LOOP ERROR] {exc!r}", flush=True)
        await asyncio.sleep(SOURCE_POLL_INTERVAL)


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
        await asyncio.sleep(SOURCE_POLL_INTERVAL)


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


async def main():
    global MY_USER_ID
    async with app:
        me = await app.get_me()
        MY_USER_ID = me.id
        print(f"[LOGGED IN] id={MY_USER_ID}", flush=True)

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

        if ENABLE_STARTUP_RECOVERY:
            for chat_id in COMMENT_GROUPS:
                await recover_recent_discussion_root(chat_id)

        for chat_id in DELETE_GROUPS:
            await index_recent_own_messages(chat_id)

        print("[STARTED]", flush=True)
        await idle()


app.run(main())
