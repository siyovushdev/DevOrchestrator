#!/usr/bin/env python3

import getpass
import json
import os
import urllib.parse
import urllib.request
from pathlib import Path


CONFIG_DIR = Path.home() / ".config/devorchestrator"
ENV_FILE = CONFIG_DIR / "telegram.env"


def api(token, method, data=None):
    url = f"https://api.telegram.org/bot{token}/{method}"

    payload = None
    if data is not None:
        payload = urllib.parse.urlencode(data).encode()

    request = urllib.request.Request(url, data=payload)

    with urllib.request.urlopen(request, timeout=15) as response:
        result = json.loads(response.read())

    if not result.get("ok"):
        raise RuntimeError(result)

    return result["result"]


def main():
    print("Telegram DevOrchestrator setup")
    print("Сначала отправь /start своему боту в Telegram.")
    print()

    token = getpass.getpass("Bot token (input hidden): ").strip()

    me = api(token, "getMe")
    print(f"Bot: @{me.get('username')}")

    updates = api(token, "getUpdates")

    chat = None

    for update in reversed(updates):
        message = (
            update.get("message")
            or update.get("edited_message")
        )

        if not message:
            continue

        candidate = message.get("chat") or {}

        if candidate.get("type") == "private":
            chat = candidate
            break

    if not chat:
        raise SystemExit(
            "Не найден private chat. "
            "Отправь боту /start и запусти setup ещё раз."
        )

    chat_id = chat["id"]

    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_DIR.chmod(0o700)

    ENV_FILE.write_text(
        f"TELEGRAM_BOT_TOKEN={token}\n"
        f"TELEGRAM_CHAT_ID={chat_id}\n"
    )

    ENV_FILE.chmod(0o600)

    api(
        token,
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": (
                "✅ DevOrchestrator connected\n\n"
                "WORK → Telegram notification channel is ready."
            ),
        },
    )

    print()
    print(f"Chat ID: {chat_id}")
    print(f"Saved securely: {ENV_FILE}")
    print("TEST MESSAGE SENT")


if __name__ == "__main__":
    main()
