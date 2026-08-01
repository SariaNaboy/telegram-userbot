import os
import asyncio
from pyrogram import Client, raw
from pyrogram.raw.types import PeerUser, PeerChannel, InputChannel

API_ID   = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION  = os.getenv("SESSION_STRING")

TARGET_CHATS = {-1002866597350, -1003984885147}
TRIGGER_WORDS = {"گزارش", "report", "@admin", "صیک", "سیک", "اخطار", "بن", "سکوت", "ban", "mute"}

app = Client(
    "userbot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION,
    sleep_threshold=0,
    workers=12
)

my_messages: dict[int, set[int]] = {}
channel_cache: dict[int, InputChannel] = {}
MY_USER_ID: int | None = None


async def get_input_channel(client: Client, chat_id: int) -> InputChannel:
    if chat_id not in channel_cache:
        peer = await client.resolve_peer(chat_id)
        channel_cache[chat_id] = InputChannel(
            channel_id=peer.channel_id,
            access_hash=peer.access_hash
        )
    return channel_cache[chat_id]


@app.on_raw_update()
async def ultra_fast(client, update, users, chats):
    global MY_USER_ID

    if not isinstance(update, raw.types.UpdateNewChannelMessage):
        return

    msg = update.message
    if not isinstance(msg, raw.types.Message):
        return

    peer = getattr(msg, "peer_id", None)
    if not isinstance(peer, PeerChannel):
        return

    chat_id = -(1_000_000_000_000 + peer.channel_id)
    if chat_id not in TARGET_CHATS:
        return

    is_mine = False

    if getattr(msg, "out", False):
        is_mine = True
    else:
        from_id = getattr(msg, "from_id", None)
        if isinstance(from_id, PeerUser):
            if MY_USER_ID is None:
                MY_USER_ID = (await client.get_me()).id
            is_mine = (from_id.user_id == MY_USER_ID)
        elif isinstance(from_id, PeerChannel):
            is_mine = (from_id.channel_id == peer.channel_id)

    if is_mine:
        my_messages.setdefault(chat_id, set()).add(msg.id)
        return

    reply = getattr(msg, "reply_to", None)
    if not reply:
        return

    replied_id = getattr(reply, "reply_to_msg_id", None)
    if not replied_id or replied_id not in my_messages.get(chat_id, set()):
        return

    text = (getattr(msg, "message", "") or "").lower()
    if not any(w in text for w in TRIGGER_WORDS):
        return

    try:
        input_channel = await get_input_channel(client, chat_id)
        await client.invoke(
            raw.functions.channels.DeleteMessages(
                channel=input_channel,
                id=[replied_id]
            )
        )
        my_messages[chat_id].discard(replied_id)
    except Exception as e:
        print(f"[ERROR] {e}")


app.run()
