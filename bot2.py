# -*- coding: utf-8 -*-
"""
bot2 — ربات دوم (روی همان Actions ولی با اکانت/محل/متن متفاوت)

- فقط روی پست‌هایی که متنشان شامل کلمهٔ «تجربه» است کامنت می‌گذارد
- ۱۰ ثانیه بعد از تشخیص (تأخیر انسانی) کامنت 🫠🫠 را روی ریشهٔ بحث می‌گذارد
- پولینگ فعال + fallback پوش + مدیریت FloodWait (پست هیچ‌وقت گم نمی‌شود)
"""
import os
import time
import asyncio
import secrets
import random
import traceback
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

from pyrogram import Client, filters, raw
from pyrogram.errors import FloodWait
from pyrogram.raw.types import (
    PeerChannel,
    InputPeerChannel,
    InputChannel,
    UpdateNewChannelMessage,
)

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION = os.environ["SESSION_STRING"]

# ===== کانفیگ ربات دوم =====
SOURCE_CHANNELS = {-1002061774069}        # کانال: 2061774069
COMMENT_GROUPS = {-1002118429464}         # گروه:  2118429464
TRIGGER_WORD = "تجربه"                     # فقط این کلمه
COMMENT_TEXT = "🫠🫠"
DELAY_SECONDS = int(os.getenv("DELAY_SECONDS", "10"))   # تأخیر قبل از کامنت
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "15.0"))

app = Client(
    "bot2",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION,
    sleep_threshold=0,
    workers=16,
)

peer_cache = {}
last_seen_channel_post = {}   # chat -> آخرین پست دیده‌شده
commented_posts = set()       # (chat, msg_id) — ددپلیکیت
recently_mapped = {}          # جلوگیری از get_discussion_message تکراری


def chat_id_from_channel_id(channel_id: int) -> int:
    return -(1_000_000_000_000 + channel_id)


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


async def get_post_text(item):
    """متن پیام (شامل کپشن مدیا) را برمی‌گرداند."""
    for attr in ("text", "caption"):
        val = getattr(item, attr, None)
        if val:
            return str(val)
    return ""


async def map_to_root(source_chat_id: int, source_message_id: int):
    """پست کانال را به ریشهٔ بحث در گروه وصل می‌کند (با FloodWait retry)."""
    key = (source_chat_id, source_message_id)
    now = time.monotonic()
    if key in recently_mapped and now - recently_mapped[key] < 120:
        return True

    max_retries = 3
    for attempt in range(max_retries):
        try:
            root = await app.get_discussion_message(source_chat_id, source_message_id)
            break
        except FloodWait as exc:
            wait = exc.value + 1
            print(
                f"[MAP FLOOD] {key} wait={wait}s attempt={attempt + 1}",
                flush=True,
            )
            await asyncio.sleep(wait)
    else:
        print(f"[MAP FAILED] {key} — will retry next cycle", flush=True)
        return False

    discussion_chat_id = root.chat.id
    if discussion_chat_id not in COMMENT_GROUPS:
        print(
            f"[MAP IGNORED] source={source_chat_id}/{source_message_id} "
            f"discussion_chat={discussion_chat_id}",
            flush=True,
        )
        recently_mapped[key] = now
        return True

    print(
        f"[MAP OK] source={source_chat_id}/{source_message_id} "
        f"root={discussion_chat_id}/{root.id}",
        flush=True,
    )
    recently_mapped[key] = now

    # اگر قبلاً روی این ریشه کامنت گذاشتیم، دوباره نگذار
    root_key = (discussion_chat_id, root.id)
    if root_key in commented_posts:
        return True

    commented_posts.add(root_key)
    await asyncio.sleep(DELAY_SECONDS)  # تأخیر ۱۰ ثانیه‌ای
    await send_comment(discussion_chat_id, root.id)
    return True


async def send_comment(chat_id: int, root_message_id: int):
    try:
        peer, _ = await get_channel_peers(chat_id)
        print(
            f"[COMMENT ATTEMPT] chat={chat_id} root={root_message_id} "
            f"text={COMMENT_TEXT!r}",
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
            f"type={type(result).__name__}",
            flush=True,
        )
    except FloodWait as exc:
        print(f"[COMMENT FLOOD] wait={exc.value}s", flush=True)
        await asyncio.sleep(exc.value + 1)
        try:
            peer, _ = await get_channel_peers(chat_id)
            result = await app.invoke(
                raw.functions.messages.SendMessage(
                    peer=peer,
                    message=COMMENT_TEXT,
                    random_id=secrets.randbits(63),
                    reply_to_msg_id=root_message_id,
                    no_webpage=True,
                )
            )
            print(f"[COMMENT SENT AFTER FLOOD] chat={chat_id} root={root_message_id}", flush=True)
        except Exception as retry_exc:
            print(f"[COMMENT RETRY ERROR] {chat_id}/{root_message_id}: {retry_exc!r}", flush=True)
    except Exception as exc:
        print(f"[COMMENT ERROR] {chat_id}/{root_message_id}: {exc!r}", flush=True)
        print(traceback.format_exc(), flush=True)


async def poll_source_channels():
    """هر POLL_INTERVAL ثانیه کانال را می‌پرسد؛ پست‌های جدیدِ دارای «تجربه» را کامنت می‌کند."""
    while True:
        try:
            for chat_id in SOURCE_CHANNELS:
                last = last_seen_channel_post.get(chat_id, 0)
                try:
                    async for item in app.get_chat_history(chat_id, limit=5):
                        if item.id <= last:
                            break
                        print(f"[NEW POST] {chat_id}/{item.id}", flush=True)
                        text = await get_post_text(item)
                        if TRIGGER_WORD in text:
                            print(
                                f"[TRIGGER FOUND] {chat_id}/{item.id} "
                                f"text={text[:60]!r}",
                                flush=True,
                            )
                            ok = await map_to_root(chat_id, item.id)
                            if not ok:
                                print(
                                    f"[DEFERRED] {chat_id}/{item.id} — retry next cycle",
                                    flush=True,
                                )
                                break
                        else:
                            print(
                                f"[NO TRIGGER] {chat_id}/{item.id} "
                                f"text={text[:40]!r}",
                                flush=True,
                            )
                        last_seen_channel_post[chat_id] = max(
                            last_seen_channel_post.get(chat_id, 0), item.id
                        )
                except Exception as exc:
                    print(f"[POLL CHANNEL ERROR] {chat_id}: {exc!r}", flush=True)
        except Exception as exc:
            print(f"[POLL LOOP ERROR] {exc!r}", flush=True)
        await asyncio.sleep(POLL_INTERVAL + random.uniform(0, 3))


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
    if chat_id not in SOURCE_CHANNELS:
        return

    msg_text = (getattr(message, "message", "") or "")
    if TRIGGER_WORD not in msg_text:
        return

    message_id = message.id
    if message_id <= last_seen_channel_post.get(chat_id, 0):
        return
    print(f"[RAW TRIGGER] {chat_id}/{message_id}", flush=True)
    last_seen_channel_post[chat_id] = max(last_seen_channel_post.get(chat_id, 0), message_id)
    asyncio.create_task(map_to_root(chat_id, message_id))


async def main():
    async with app:
        me = await app.get_me()
        print(f"[BOT2 LOGGED IN] id={me.id} username={me.username}", flush=True)

        for cid in SOURCE_CHANNELS:
            try:
                async for item in app.get_chat_history(cid, limit=1):
                    last_seen_channel_post[cid] = item.id
                    print(f"[BASELINE] channel={cid} top_id={item.id}", flush=True)
                    break
            except Exception as exc:
                print(f"[BASELINE ERROR] {cid}: {exc!r}", flush=True)

        for cid in COMMENT_GROUPS:
            try:
                await get_channel_peers(cid)
                print(f"[READY] group={cid}", flush=True)
            except Exception as exc:
                print(f"[PREWARM ERROR] {cid}: {exc!r}", flush=True)

        asyncio.create_task(poll_source_channels())
        print("[BOT2 STARTED]", flush=True)
        await asyncio.Event().wait()


app.run(main())
