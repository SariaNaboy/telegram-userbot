import os
import asyncio
from pprint import pprint

from pyrogram import Client, raw

CHAT_ID = -1001596320253
ROOT_ID = 178049801


def compact(obj):
    if obj is None:
        return None
    return getattr(obj, "__dict__", repr(obj))


async def inspect(app, peer, message_id, label):
    print(f"\n=== {label}: HIGH LEVEL ===")
    message = await app.get_messages(CHAT_ID, message_id)
    print({
        "id": getattr(message, "id", None),
        "empty": getattr(message, "empty", None),
        "outgoing": getattr(message, "outgoing", None),
        "text": getattr(message, "text", None),
        "caption": getattr(message, "caption", None),
        "from_user": compact(getattr(message, "from_user", None)),
        "sender_chat": compact(getattr(message, "sender_chat", None)),
        "forward_from_chat": compact(getattr(message, "forward_from_chat", None)),
        "reply_to_message_id": getattr(message, "reply_to_message_id", None),
        "reply_to_top_message_id": getattr(message, "reply_to_top_message_id", None),
    })

    result = await app.invoke(
        raw.functions.channels.GetMessages(
            channel=peer,
            id=[raw.types.InputMessageID(id=message_id)],
        )
    )
    raw_message = result.messages[0]
    print(f"=== {label}: RAW ===")
    print(type(raw_message).__name__)
    pprint(getattr(raw_message, "__dict__", repr(raw_message)))
    return raw_message


async def main():
    app = Client(
        "debug_inspector",
        api_id=int(os.environ["API_ID"]),
        api_hash=os.environ["API_HASH"],
        session_string=os.environ["SESSION_STRING"],
    )
    async with app:
        me = await app.get_me()
        print("=== ACCOUNT ===")
        print({"id": me.id, "username": me.username})

        peer = await app.resolve_peer(CHAT_ID)
        root = await inspect(app, peer, ROOT_ID, "ROOT")
        latest_reply_id = getattr(getattr(root, "replies", None), "max_id", None)
        print(f"=== ROOT LATEST REPLY ID: {latest_reply_id} ===")
        if latest_reply_id:
            await inspect(app, peer, latest_reply_id, "LATEST_REPLY")


asyncio.run(main())
