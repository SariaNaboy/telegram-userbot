import os
import asyncio
import secrets
from collections import defaultdict

from pyrogram import Client, idle, raw
from pyrogram.errors import FloodWait
from pyrogram.raw.types import (
    PeerChannel,
    UpdateNewChannelMessage,
    InputPeerChannel,
    InputChannel,
)


API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION = os.environ["SESSION_STRING"]

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "thisisatestforvaset")

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
    "گزارش",
    "report",
    "@admin",
    "صیک",
    "سیک",
    "اخطار",
    "بن",
    "سکوت",
    "ban",
    "mute",
}

COMMENT_TEXT = "🤔🤔🤔🤔"

app = Client(
    "userbot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION,
    sleep_threshold=0,
    workers=32,
)

my_messages = defaultdict(set)
commented_posts = set()
peer_cache = {}


def channel_id_to_chat_id(channel_id: int) -> int:
    return -(1_000_000_000_000 + channel_id)


async def get_channel_peers(chat_id: int):
    cached = peer_cache.get(chat_id)
    if cached:
        return cached

    peer = await app.resolve_peer(chat_id)

    if not isinstance(peer, InputPeerChannel):
        raise TypeError(f"{chat_id} یک channel/supergroup نیست: {type(peer).__name__}")

    channel = InputChannel(
        channel_id=peer.channel_id,
        access_hash=peer.access_hash,
    )

    peer_cache[chat_id] = (peer, channel)
    return peer, channel


async def notify_admin(text: str):
    try:
        await app.send_message(ADMIN_USERNAME, text)
    except Exception as e:
        print(f"[ADMIN ERROR] {type(e).__name__}: {e}")


async def send_comment_fast(chat_id: int, reply_to_id: int):
    post_key = (chat_id, reply_to_id)

    try:
        peer, _ = await get_channel_peers(chat_id)

        await app.invoke(
            raw.functions.messages.SendMessage(
                peer=peer,
                message=COMMENT_TEXT,
                random_id=secrets.randbits(63),
                reply_to_msg_id=reply_to_id,
                no_webpage=True,
            )
        )

        print(f"[COMMENT] {chat_id} -> {reply_to_id}")

        internal_chat_id = str(chat_id)[4:]
        asyncio.create_task(
            notify_admin(
                f"کامنت ثبت شد:\n"
                f"https://t.me/c/{internal_chat_id}/{reply_to_id}"
            )
        )

    except FloodWait as e:
        print(f"[COMMENT FLOOD] wait={e.value}s | {chat_id} -> {reply_to_id}")
        await asyncio.sleep(e.value + 1)

        try:
            peer, _ = await get_channel_peers(chat_id)
            await app.invoke(
                raw.functions.messages.SendMessage(
                    peer=peer,
                    message=COMMENT_TEXT,
                    random_id=secrets.randbits(63),
                    reply_to_msg_id=reply_to_id,
                    no_webpage=True,
                )
            )
            print(f"[COMMENT RETRY OK] {chat_id} -> {reply_to_id}")
        except Exception as retry_error:
            print(f"[COMMENT RETRY ERROR] {type(retry_error).__name__}: {retry_error}")

    except Exception as e:
        print(f"[COMMENT ERROR] {type(e).__name__}: {e}")
        commented_posts.discard(post_key)


async def delete_message_fast(chat_id: int, message_id: int):
    try:
        _, channel = await get_channel_peers(chat_id)

        await app.invoke(
            raw.functions.channels.DeleteMessages(
                channel=channel,
                id=[message_id],
            )
        )

        my_messages[chat_id].discard(message_id)
        print(f"[DELETED] {chat_id} -> {message_id}")

    except FloodWait as e:
        print(f"[DELETE FLOOD] wait={e.value}s | {chat_id} -> {message_id}")
        await asyncio.sleep(e.value + 1)

        try:
            _, channel = await get_channel_peers(chat_id)
            await app.invoke(
                raw.functions.channels.DeleteMessages(
                    channel=channel,
                    id=[message_id],
                )
            )
            my_messages[chat_id].discard(message_id)
            print(f"[DELETE RETRY OK] {chat_id} -> {message_id}")
        except Exception as retry_error:
            print(f"[DELETE RETRY ERROR] {type(retry_error).__name__}: {retry_error}")

    except Exception as e:
        print(f"[DELETE ERROR] {type(e).__name__}: {e}")


@app.on_raw_update()
async def handle_raw_update(client, update, users, chats):
    if not isinstance(update, UpdateNewChannelMessage):
        return

    msg = update.message

    if not isinstance(msg, raw.types.Message):
        return

    peer_id = getattr(msg, "peer_id", None)

    if not isinstance(peer_id, PeerChannel):
        return

    chat_id = channel_id_to_chat_id(peer_id.channel_id)
    message_id = msg.id

    if chat_id in COMMENT_GROUPS:
        fwd_from = getattr(getattr(msg, "fwd_from", None), "from_id", None)

        if isinstance(fwd_from, PeerChannel):
            post_key = (chat_id, message_id)

            if post_key not in commented_posts:
                commented_posts.add(post_key)
                asyncio.create_task(send_comment_fast(chat_id, message_id))

            return

    if chat_id not in DELETE_GROUPS:
        return

    if getattr(msg, "out", False):
        my_messages[chat_id].add(message_id)
        return

    reply_to = getattr(msg, "reply_to", None)
    replied_id = getattr(reply_to, "reply_to_msg_id", None) if reply_to else None

    if not replied_id or replied_id not in my_messages[chat_id]:
        return

    text = (getattr(msg, "message", "") or "").casefold()

    if not any(word.casefold() in text for word in TRIGGER_WORDS):
        return

    asyncio.create_task(delete_message_fast(chat_id, replied_id))


async def main():
    async with app:
        for chat_id in DELETE_GROUPS | COMMENT_GROUPS:
            try:
                await get_channel_peers(chat_id)
                print(f"[READY] {chat_id}")
            except Exception as e:
                print(f"[PREWARM ERROR] {chat_id}: {type(e).__name__}: {e}")

        print("Userbot started.")
        await idle()


app.run(main())
