import os
import time
import asyncio
import secrets
import traceback
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
WAIT_FOR_FIRST_COMMENT = 120
OWN_MESSAGE_HISTORY_LIMIT = 1000
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
# root_key = (discussion_chat_id, forwarded_root_message_id)
waiting_roots = {}
comment_attempted = set()
thread_watchers = {}
recovery_checked = set()
logged_reply_roots = set()


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
            await asyncio.sleep(1)

        waiting_roots.pop(root_key, None)
        print(f"[WATCHER TIMEOUT] {root_key}", flush=True)
    finally:
        thread_watchers.pop(root_key, None)


async def observe_discussion_root(chat_id: int, root_message_id: int, source_channel_id=None, known_count=None, source="unknown"):
    """Common root entry point used by high-level and raw handlers."""
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


# This is the main fix. The supplied high-level Message already contains
# forward_from_chat and replies, whereas raw update delivery can be missed when
# an ephemeral runner starts late.
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

    try:
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

        for chat_id in COMMENT_GROUPS:
            await recover_recent_discussion_root(chat_id)
        for chat_id in DELETE_GROUPS:
            await index_recent_own_messages(chat_id)

        print("[STARTED]", flush=True)
        await idle()


app.run(main())
