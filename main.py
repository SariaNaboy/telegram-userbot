import os
import time
import asyncio
import secrets
from collections import defaultdict

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

# پشتیبانی از نام فعلی secret در workflow و نام استاندارد env.
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME") or os.getenv("admin_username")

DELETE_GROUPS = {
    -1002866597350,
    -1003984885147,
    -1001596320253,
}

COMMENT_GROUPS = {
    -1003984885147,
    -1001596320253,
}

TRIGGER_WORDS = {
    "گزارش", "report", "@admin", "صیک", "سیک",
    "اخطار", "بن", "سکوت", "ban", "mute",
}

COMMENT_TEXT = "🤔🤔🤔🤔"
# اگر تا این مدت هیچ‌کس کامنت نگذارد، ما هم کامنت نمی‌گذاریم.
WAIT_FOR_FIRST_COMMENT = 120
# پیام‌های اخیر خودت را در شروع هر runner ثبت می‌کنیم؛ Actionها حافظهٔ دائمی ندارند.
OWN_MESSAGE_HISTORY_LIMIT = 1000
# On GitHub Actions startup, inspect this many recent messages to recover the
# currently active discussion post that may have arrived before the runner.
COMMENT_RECOVERY_HISTORY_LIMIT = 1500

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
# {(discussion_chat_id, forwarded_root_message_id): deadline}
waiting_roots = {}
# هر root فقط یک بار برای کامنت تلاش می‌شود.
comment_attempted = set()
# Roots recovered from an out-of-order reply update; prevents repeated lookups.
recovery_checked = set()
# One lightweight watcher per newly detected discussion root.
thread_watchers = {}


def chat_id_from_channel_id(channel_id: int) -> int:
    return -(1_000_000_000_000 + channel_id)


async def notify_admin(text: str):
    if not ADMIN_USERNAME:
        print("[ADMIN NOTIFY SKIPPED] ADMIN_USERNAME is not configured")
        return

    try:
        await app.send_message(ADMIN_USERNAME, text)
        print("[ADMIN NOTIFIED]")
    except Exception as exc:
        print(f"[ADMIN NOTIFY ERROR] {exc!r}")


async def get_channel_peers(chat_id: int):
    """Return (InputPeerChannel, InputChannel), cached after startup."""
    cached = peer_cache.get(chat_id)
    if cached is not None:
        return cached

    peer = await app.resolve_peer(chat_id)
    if not isinstance(peer, InputPeerChannel):
        raise TypeError(f"{chat_id} is not a channel/supergroup")

    channel = InputChannel(
        channel_id=peer.channel_id,
        access_hash=peer.access_hash,
    )
    peer_cache[chat_id] = (peer, channel)
    return peer, channel


async def index_recent_own_messages(chat_id: int):
    """Populate the fast cache for messages sent before this Action run started."""
    indexed = 0
    try:
        async for item in app.get_chat_history(chat_id, limit=OWN_MESSAGE_HISTORY_LIMIT):
            sender = getattr(item, "from_user", None)
            if getattr(item, "outgoing", False) or (sender is not None and sender.id == MY_USER_ID):
                my_messages[chat_id].add(item.id)
                indexed += 1
        print(f"[OWN HISTORY INDEXED] chat={chat_id} messages={indexed}")
    except Exception as exc:
        print(f"[OWN HISTORY INDEX ERROR] chat={chat_id}: {exc!r}")


async def target_is_mine(chat_id: int, message_id: int) -> bool:
    """
    Fast path is my_messages. This fallback makes messages sent before this
    process started work too.
    """
    if message_id in my_messages[chat_id]:
        return True

    try:
        target = await app.get_messages(chat_id, message_id)
    except Exception as exc:
        print(f"[TARGET LOOKUP ERROR] {chat_id}/{message_id}: {exc!r}")
        return False

    if not target or getattr(target, "empty", False):
        return False

    mine = bool(getattr(target, "outgoing", False))
    sender = getattr(target, "from_user", None)
    mine = mine or (sender is not None and sender.id == MY_USER_ID)

    if mine:
        my_messages[chat_id].add(message_id)

    return mine


async def delete_now(chat_id: int, message_id: int):
    try:
        peer, _ = await get_channel_peers(chat_id)
        await app.invoke(
            raw.functions.channels.DeleteMessages(
                channel=peer,
                id=[message_id],
            )
        )
        my_messages[chat_id].discard(message_id)
        print(f"[DELETED] {chat_id}/{message_id}")

    except FloodWait as exc:
        # Deleting late is still better than not deleting at all.
        print(f"[DELETE FLOOD] wait={exc.value}s {chat_id}/{message_id}")
        await asyncio.sleep(exc.value + 1)
        try:
            peer, _ = await get_channel_peers(chat_id)
            await app.invoke(
                raw.functions.channels.DeleteMessages(
                    channel=peer,
                    id=[message_id],
                )
            )
            my_messages[chat_id].discard(message_id)
            print(f"[DELETED AFTER FLOOD] {chat_id}/{message_id}")
        except Exception as retry_exc:
            print(f"[DELETE RETRY ERROR] {chat_id}/{message_id}: {retry_exc!r}")

    except Exception as exc:
        print(f"[DELETE ERROR] {chat_id}/{message_id}: {exc!r}")


async def poll_latest_discussion_root(chat_id: int):
    """
    Last-resort live safety net. In addition to updates, inspect the newest
    discussion root once per second. This covers roots/replies that an update
    handler never associated while keeping the polling scope to one root.
    """
    try:
        peer, _ = await get_channel_peers(chat_id)
        while True:
            try:
                result = await app.invoke(
                    raw.functions.messages.GetHistory(
                        peer=peer,
                        offset_id=0,
                        offset_date=0,
                        add_offset=0,
                        limit=100,
                        max_id=0,
                        min_id=0,
                        hash=0,
                    )
                )
                latest_root = None
                for item in getattr(result, "messages", []):
                    if not isinstance(item, raw.types.Message):
                        continue
                    fwd_from = getattr(getattr(item, "fwd_from", None), "from_id", None)
                    if isinstance(fwd_from, PeerChannel):
                        latest_root = item
                        break

                if latest_root is not None:
                    root_key = (chat_id, latest_root.id)
                    reply_count = getattr(getattr(latest_root, "replies", None), "replies", 0)
                    if reply_count >= 1 and root_key not in comment_attempted:
                        comment_attempted.add(root_key)
                        waiting_roots.pop(root_key, None)
                        print(f"[POLL FOUND ACTIVE ROOT] {root_key} replies={reply_count}")
                        await send_comment_after_external_reply(chat_id, latest_root.id)
            except Exception as exc:
                print(f"[POLL ERROR] chat={chat_id}: {exc!r}")

            await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        raise


async def watch_discussion_root(chat_id: int, root_message_id: int):
    """
    Safety net for a busy group: if a raw reply update is delayed or processed
    out of order, poll the documented server-side reply count until the first
    comment appears. It never sends while the count is zero.
    """
    root_key = (chat_id, root_message_id)
    deadline = time.monotonic() + WAIT_FOR_FIRST_COMMENT

    try:
        while time.monotonic() < deadline:
            if root_key in comment_attempted:
                return

            try:
                count = await app.get_discussion_replies_count(chat_id, root_message_id)
                if count >= 1:
                    if root_key not in comment_attempted:
                        comment_attempted.add(root_key)
                        waiting_roots.pop(root_key, None)
                        print(f"[WATCHER FOUND FIRST REPLY] {root_key} count={count}")
                        await send_comment_after_external_reply(chat_id, root_message_id)
                    return
            except Exception as exc:
                # Keep watching: transient RPC failures must not make us miss the post.
                print(f"[WATCHER COUNT ERROR] {root_key}: {exc!r}")

            await asyncio.sleep(1.0)

        print(f"[WATCHER TIMEOUT] {root_key}")
    finally:
        thread_watchers.pop(root_key, None)


async def find_recent_active_discussion_root(chat_id: int):
    """Find the newest forwarded channel post that already has at least one reply."""
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
                return item.id

        ids = [item.id for item in messages if hasattr(item, "id")]
        if not ids:
            return None
        offset_id = min(ids)
        scanned += len(messages)

    return None


async def recover_recent_discussion_root(chat_id: int):
    """Startup recovery for posts that appeared before this Action runner started."""
    try:
        root_id = await find_recent_active_discussion_root(chat_id)
        if root_id is None:
            print(f"[NO RECENT ACTIVE ROOT] chat={chat_id}")
            return

        root_key = (chat_id, root_id)
        if root_key in comment_attempted:
            return

        comment_attempted.add(root_key)
        print(f"[STARTUP ROOT RECOVERY] {root_key}")
        await send_comment_after_external_reply(chat_id, root_id)
    except Exception as exc:
        print(f"[STARTUP ROOT RECOVERY ERROR] chat={chat_id}: {exc!r}")


async def recover_root_from_reply(chat_id: int, root_message_id: int):
    """Fallback for busy groups when a reply update is processed before its root update."""
    root_key = (chat_id, root_message_id)
    if root_key in comment_attempted or root_key in recovery_checked:
        return

    recovery_checked.add(root_key)
    try:
        root = await app.get_messages(chat_id, root_message_id)
        # An automatic discussion post is represented as a forward from a channel.
        if not root or not getattr(root, "forward_from_chat", None):
            return

        comment_attempted.add(root_key)
        print(f"[RECOVERED DISCUSSION ROOT] {root_key}")
        await send_comment_after_external_reply(chat_id, root_message_id)
    except Exception as exc:
        print(f"[ROOT RECOVERY ERROR] {root_key}: {exc!r}")


async def send_comment_after_external_reply(chat_id: int, root_message_id: int):
    """
    This runs only after an incoming reply to the root was received. That update
    is already a server-confirmed earlier comment, so another count RPC is both
    unnecessary and costly in a busy discussion group.
    """
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
        print(f"[COMMENT SENT] {chat_id}/{root_message_id}")

        # Notification never blocks the comment send.
        internal_chat_id = str(chat_id)[4:] if str(chat_id).startswith("-100") else str(abs(chat_id))
        asyncio.create_task(
            notify_admin(
                "کامنت ثبت شد:\n"
                f"https://t.me/c/{internal_chat_id}/{root_message_id}"
            )
        )

    except FloodWait as exc:
        # A delayed retry is not useful for the intended fast-comment behavior.
        print(f"[COMMENT SKIPPED: FLOOD {exc.value}s] {chat_id}/{root_message_id}")
    except Exception as exc:
        print(f"[COMMENT ERROR] {chat_id}/{root_message_id}: {exc!r}")


@app.on_message(filters.chat(list(DELETE_GROUPS)) & filters.outgoing)
async def remember_outgoing_message(client, message):
    """High-level backup: ensures normal outgoing group messages enter the fast cache."""
    my_messages[message.chat.id].add(message.id)
    print(f"[MY MESSAGE HIGH LEVEL] {message.chat.id}/{message.id}")


@app.on_raw_update()
async def on_raw_update(client, update, users, chats):
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

    # ----- Comment logic: observe a forwarded channel post, but DO NOT send. -----
    if chat_id in COMMENT_GROUPS:
        now = time.monotonic()
        for key, deadline in list(waiting_roots.items()):
            if deadline <= now:
                waiting_roots.pop(key, None)
                print(f"[COMMENT TIMEOUT] {key}")

        fwd_from = getattr(getattr(message, "fwd_from", None), "from_id", None)
        if isinstance(fwd_from, PeerChannel):
            root_key = (chat_id, message_id)
            if root_key not in comment_attempted:
                waiting_roots.setdefault(root_key, now + WAIT_FOR_FIRST_COMMENT)
                if root_key not in thread_watchers:
                    thread_watchers[root_key] = asyncio.create_task(
                        watch_discussion_root(chat_id, message_id)
                    )
                print(f"[WAITING FOR FIRST COMMENT] {chat_id}/{message_id}")
            return

        # Only an incoming reply to one of our tracked roots starts the send.
        if not is_outgoing:
            reply_header = getattr(message, "reply_to", None)
            reply_id = getattr(reply_header, "reply_to_msg_id", None) if reply_header else None
            top_id = getattr(reply_header, "reply_to_top_id", None) if reply_header else None

            root_key = None
            for candidate in waiting_roots:
                candidate_chat, candidate_root = candidate
                if candidate_chat == chat_id and (reply_id == candidate_root or top_id == candidate_root):
                    root_key = candidate
                    break

            if root_key is not None:
                waiting_roots.pop(root_key, None)
                comment_attempted.add(root_key)
                print(f"[EXTERNAL COMMENT SEEN] {root_key}")

                # The external update is already proof that a comment exists on Telegram.
                await send_comment_after_external_reply(chat_id, root_key[1])

            elif top_id or reply_id:
                # Busy groups can deliver/process a reply before the root forward has
                # entered waiting_roots. Recover the root once instead of missing it.
                await recover_root_from_reply(chat_id, top_id or reply_id)

    # ----- Delete logic -----
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
        print(f"[MY MESSAGE] {chat_id}/{message_id}")
        return

    reply_header = getattr(message, "reply_to", None)
    replied_id = getattr(reply_header, "reply_to_msg_id", None) if reply_header else None
    if not replied_id:
        return

    text = (getattr(message, "message", "") or "").casefold()
    if not any(word.casefold() in text for word in TRIGGER_WORDS):
        return

    # This fallback also supports your messages from before process startup.
    if not await target_is_mine(chat_id, replied_id):
        print(f"[TRIGGER IGNORED: TARGET NOT MINE] {chat_id}/{replied_id}")
        return

    print(f"[DELETE TRIGGER] {chat_id}/{replied_id} by reply={message_id}")
    await delete_now(chat_id, replied_id)


async def main():
    global MY_USER_ID

    async with app:
        me = await app.get_me()
        MY_USER_ID = me.id
        print(f"[LOGGED IN] id={MY_USER_ID}")

        for chat_id in DELETE_GROUPS | COMMENT_GROUPS:
            try:
                await get_channel_peers(chat_id)
                print(f"[READY] {chat_id}")
            except Exception as exc:
                print(f"[PREWARM ERROR] {chat_id}: {exc!r}")

        # Recover an active discussion post that was published before this
        # ephemeral GitHub Actions runner connected.
        for chat_id in COMMENT_GROUPS:
            await recover_recent_discussion_root(chat_id)

        # Independent one-second fallback: raw updates remain the fast path,
        # while this prevents a busy group from losing a root entirely.
        for chat_id in COMMENT_GROUPS:
            asyncio.create_task(poll_latest_discussion_root(chat_id))

        # GitHub Actions بعد از هر اجرا از نو شروع می‌شود؛ پس پیام‌های
        # عادیِ اخیر را یک‌بار index می‌کنیم تا حذف آن‌ها بدون lookup اضافی باشد.
        for chat_id in DELETE_GROUPS:
            await index_recent_own_messages(chat_id)

        print("[STARTED]")
        await idle()


app.run(main())
