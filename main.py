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


def chat_id_from_channel_id(channel_id: int) -> int:
    return -(1_000_000_000_000 + channel_id)


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


async def verify_then_comment(chat_id: int, root_message_id: int):
    """
    A server-side count is required. Never send while Telegram reports zero
    replies, which prevents this client from intentionally becoming first.
    """
    try:
        count = await app.get_discussion_replies_count(chat_id, root_message_id)
        print(f"[COMMENT COUNT] {chat_id}/{root_message_id}: {count}")

        if count < 1:
            print(f"[COMMENT CANCELLED: ZERO REPLIES] {chat_id}/{root_message_id}")
            return

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

    except FloodWait as exc:
        # Do NOT retry comments: a delayed retry is no longer a fast comment.
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
                print(f"[WAITING FOR FIRST COMMENT] {chat_id}/{message_id}")
            return

        # Only an incoming reply to one of our tracked roots starts verification.
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

                # No intentional delay. Verification is a server-side safety check.
                await verify_then_comment(chat_id, root_key[1])

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

        # GitHub Actions بعد از هر اجرا از نو شروع می‌شود؛ پس پیام‌های
        # عادیِ اخیر را یک‌بار index می‌کنیم تا حذف آن‌ها بدون lookup اضافی باشد.
        for chat_id in DELETE_GROUPS:
            await index_recent_own_messages(chat_id)

        print("[STARTED]")
        await idle()


app.run(main())
