# -*- coding: utf-8 -*-
"""
listener.py — هر پیامی که به این اکانت برسد را پرینت می‌کند.
هدف: خواندن «کد ورود تلگرام» که به شکل پیام سرویس برای سشن فعال می‌آید.

فقط برای مدت LISTEN_SECONDS (پیش‌فرض ۳۰۰ ثانیه) اجرا می‌شود و خودش خارج می‌شود.
"""
import os
import re
import asyncio
from pyrogram import Client, raw

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION = os.environ["SESSION_STRING"]
LISTEN_SECONDS = int(os.getenv("LISTEN_SECONDS", "300"))

CODE_RE = re.compile(r"(?<![0-9])([0-9]{5,6})(?![0-9])")

app = Client("listener", api_id=API_ID, api_hash=API_HASH, session_string=SESSION)


def _hl_codes(text: str) -> list[str]:
    return CODE_RE.findall(text or "")


def _print_and_maybe_code(tag: str, info: str, text: str):
    print(f"[{tag}] {info} | text={text!r}", flush=True)
    for code in _hl_codes(text):
        print(f"[!!! POSSIBLE LOGIN CODE !!!] >>> {code} <<<", flush=True)


@app.on_message()
async def on_any_message(client, message):
    try:
        sender = getattr(message, "from_user", None)
        if sender is not None:
            sinfo = f"user_id={sender.id} username={sender.username!r} name={(sender.first_name or '')!r}"
        elif getattr(message, "sender_chat", None) is not None:
            sc = message.sender_chat
            sinfo = f"chat={sc.id} title={getattr(sc, 'title', None)!r}"
        else:
            sinfo = "unknown-sender"

        text = message.text or message.caption or ""
        if not text:
            text = f"<{type(message).__name__} no text>"

        # پیام‌های خود سرویس تلگرام (777000) — همان جایی که کد ورود می‌آید
        info = f"chat_id={message.chat.id} msg_id={message.id} {sinfo} date={message.date}"
        if sender is not None and sender.id == 777000:
            _print_and_maybe_code("TG-SERVICE-MSG", info, text)
        else:
            _print_and_maybe_code("MSG", info, text)
    except Exception as exc:
        print(f"[HANDLER ERROR] {exc!r}", flush=True)


@app.on_raw_update()
async def on_raw(client, update, users, chats):
    try:
        name = type(update).__name__
        # کد ورود به شکل updateServiceNotification می‌آید (متن کاملش حاوی رقم کد است)
        if name == "UpdateServiceNotification":
            text = getattr(update, "message", None) or ""
            ntype = getattr(update, "type", None) or ""
            _print_and_maybe_code("SERVICE-NOTIF", f"type={ntype!r}", text or repr(update))
    except Exception as exc:
        print(f"[RAW ERROR] {exc!r}", flush=True)


async def main():
    async with app:
        me = await app.get_me()
        print(f"[LISTENER LOGGED IN] id={me.id} username={me.username!r} phone={me.phone_number!r}", flush=True)
        print(f"[LISTENING] for {LISTEN_SECONDS}s — هر پیامی برسد پرینت می‌شود ...", flush=True)
        # ضرب‌الاجل خودمان؛ ورکفلو هم با timeout پشتیبان است
        await asyncio.sleep(LISTEN_SECONDS)
        print("[LISTENER DONE]", flush=True)


app.run(main())
