import os
import asyncio
import random
from pyrogram import Client, raw
from pyrogram.raw.types import PeerUser, PeerChannel, UpdateNewChannelMessage, InputReplyToMessage

API_ID   = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION  = os.getenv("SESSION_STRING")
ADMIN_USERNAME = os.getenv("admin_username", "@thisisatestforvaset")

# گروه‌های دیلیت (هر ۳ گروه)
DELETE_GROUPS = {-1002866597350, -1003984885147, -1001596320253}

# گروه‌های کامنت (فقط ۲ گروه)
COMMENT_GROUPS = {-1003984885147, -1001596320253}

TRIGGER_WORDS = {"گزارش", "report", "@admin", "صیک", "سیک", "اخطار", "بن", "سکوت", "ban", "mute"}
COMMENT_TEXT = "🤔🤔🤔🤔"

DELAY_MIN = 1.5
DELAY_MAX = 3.5

app = Client("userbot", api_id=API_ID, api_hash=API_HASH, session_string=SESSION, sleep_threshold=0, workers=12)

my_messages: dict[int, set[int]] = {}
peer_cache = {}
MY_USER_ID = None
sent_for = set()


async def get_input_peer(client, chat_id):
    if chat_id not in peer_cache:
        peer_cache[chat_id] = await client.resolve_peer(chat_id)
    return peer_cache[chat_id]


async def post_comment(client, chat_id, reply_to_id):
    await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    try:
        input_peer = await get_input_peer(client, chat_id)
        await client.invoke(
            raw.functions.messages.SendMessage(
                peer=input_peer,
                message=COMMENT_TEXT,
                reply_to=InputReplyToMessage(reply_to_msg_id=reply_to_id),
                random_id=random.randint(0, 2**63),
                no_webpage=True,
            )
        )
        print(f"[✅ COMMENT] {chat_id} → {reply_to_id}")
        
        # ارسال لینک کامنت به ادمین
        try:
            await client.send_message(ADMIN_USERNAME, f"کامنت گذاشته شد:\nhttps://t.me/c/{abs(chat_id)}/{reply_to_id}")
        except:
            pass
            
    except Exception as e:
        print(f"[COMMENT ERROR] {e}")
        sent_for.discard(reply_to_id)


@app.on_raw_update()
async def ultra_fast(client, update, users, chats):
    global MY_USER_ID

    if not isinstance(update, UpdateNewChannelMessage):
        return

    msg = update.message
    if not isinstance(msg, raw.types.Message):
        return

    peer = getattr(msg, "peer_id", None)
    if not isinstance(peer, PeerChannel):
        return

    chat_id = -(1_000_000_000_000 + peer.channel_id)

    # ======================== کامنت ========================
    if chat_id in COMMENT_GROUPS:
        fwd = getattr(msg, "fwd_from", None)
        if fwd and isinstance(getattr(fwd, "from_id", None), PeerChannel):
            if msg.id not in sent_for:
                sent_for.add(msg.id)
                asyncio.create_task(post_comment(client, chat_id, msg.id))
            return

    # ======================== دیلیت ========================
    if chat_id not in DELETE_GROUPS:
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
        input_peer = await get_input_peer(client, chat_id)
        await client.invoke(raw.functions.channels.DeleteMessages(channel=input_peer, id=[replied_id]))
        my_messages[chat_id].discard(replied_id)
        print(f"[🗑️ DELETED] {chat_id} → {replied_id}")
    except Exception as e:
        print(f"[DELETE ERROR] {e}")


app.run()
