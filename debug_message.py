import os
import asyncio
from pprint import pprint

from pyrogram import Client, raw

CHAT_ID = -1001596320253
MESSAGE_ID = 178049801


def compact(obj):
    if obj is None:
        return None
    data = getattr(obj, "__dict__", None)
    return data if data is not None else repr(obj)


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

        message = await app.get_messages(CHAT_ID, MESSAGE_ID)
        print("=== HIGH LEVEL MESSAGE ===")
        print({
            "id": getattr(message, "id", None),
            "empty": getattr(message, "empty", None),
            "outgoing": getattr(message, "outgoing", None),
            "text": getattr(message, "text", None),
            "caption": getattr(message, "caption", None),
            "from_user": compact(getattr(message, "from_user", None)),
            "sender_chat": compact(getattr(message, "sender_chat", None)),
            "forward_from": compact(getattr(message, "forward_from", None)),
            "forward_from_chat": compact(getattr(message, "forward_from_chat", None)),
            "forward_from_message_id": getattr(message, "forward_from_message_id", None),
            "reply_to_message_id": getattr(message, "reply_to_message_id", None),
            "reply_to_top_message_id": getattr(message, "reply_to_top_message_id", None),
            "service": getattr(message, "service", None),
            "media": getattr(message, "media", None),
        })

        peer = await app.resolve_peer(CHAT_ID)
        result = await app.invoke(
            raw.functions.channels.GetMessages(
                channel=peer,
                id=[raw.types.InputMessageID(id=MESSAGE_ID)],
            )
        )

        print("=== RAW RESULT TYPE ===")
        print(type(result).__name__)
        print("=== RAW MESSAGE ===")
        for item in getattr(result, "messages", []):
            print(type(item).__name__)
            pprint(getattr(item, "__dict__", repr(item)))


asyncio.run(main())
