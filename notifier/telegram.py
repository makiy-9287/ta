"""
Telegram transport.

Raw Bot API over aiohttp - no framework. Long polling for commands, HTML
messages out. Only the configured chat id is allowed to issue commands.
"""
from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, List, Optional, Tuple

import aiohttp

from core.utils import get_logger

log = get_logger("telegram")

MAX_LEN = 4000
CommandHandler = Callable[[str, List[str]], Awaitable[Optional[str]]]

COMMANDS = [
    ("status", "Engine status and open setups"),
    ("active", "Detailed view of running setups"),
    ("watchlist", "Current volume-filtered universe"),
    ("zones", "A/A+ zones - /zones BTCUSDT"),
    ("signals", "Recent signals - /signals 10"),
    ("pnl", "Performance - /pnl today|week|month|all"),
    ("report", "Full report - /report week"),
    ("stats", "Engine internals and hit rates"),
    ("why", "Why a symbol has not fired - /why BTCUSDT"),
    ("health", "Connections, rate-limit and memory"),
    ("close", "Force-close a setup - /close 12"),
    ("pause", "Stop generating new signals"),
    ("resume", "Resume signal generation"),
    ("help", "Command list"),
]


class TelegramBot:
    def __init__(self, token: str, chat_id: str, poll_timeout: int = 25,
                 command_timeout: int = 45):
        self.token = token
        self.chat_id = str(chat_id)
        self.poll_timeout = poll_timeout
        self.command_timeout = command_timeout
        self.base = f"https://api.telegram.org/bot{token}"
        self._session: Optional[aiohttp.ClientSession] = None
        self._offset = 0
        self._running = False
        self.sent = 0
        self.errors = 0

    async def start(self) -> None:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.poll_timeout + 20))
        await self.register_commands()

    async def close(self) -> None:
        self._running = False
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _call(self, method: str, payload: dict, retries: int = 3):
        if self._session is None or self._session.closed:
            await self.start()
        url = f"{self.base}/{method}"
        for attempt in range(retries):
            try:
                async with self._session.post(url, json=payload) as resp:
                    data = await resp.json()
                    if resp.status == 429:
                        wait = float(data.get("parameters", {}).get("retry_after", 3))
                        await asyncio.sleep(wait + 0.5)
                        continue
                    if not data.get("ok"):
                        log.warning("telegram %s failed: %s", method, str(data)[:200])
                        return None
                    return data.get("result")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.errors += 1
                log.debug("telegram %s error: %s", method, exc)
                await asyncio.sleep(1.5 * (attempt + 1))
        return None

    # ---------------------------------------------------------------- sending
    async def send(self, text: str, chat_id: Optional[str] = None, silent: bool = False) -> None:
        target = str(chat_id or self.chat_id)
        for chunk in _split(text):
            await self._call("sendMessage", {
                "chat_id": target,
                "text": chunk,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
                "disable_notification": silent,
            })
            self.sent += 1
            await asyncio.sleep(0.05)

    async def register_commands(self) -> None:
        await self._call("setMyCommands", {
            "commands": [{"command": c, "description": d} for c, d in COMMANDS]})

    async def verify(self) -> Tuple[bool, str]:
        me = await self._call("getMe", {})
        if not me:
            return False, "getMe failed - check TELEGRAM_BOT_TOKEN"
        return True, f"@{me.get('username', 'bot')}"

    # ---------------------------------------------------------------- polling
    async def poll(self, handler: CommandHandler) -> None:
        self._running = True
        log.info("telegram polling started")
        while self._running:
            try:
                updates = await self._call("getUpdates", {
                    "offset": self._offset,
                    "timeout": self.poll_timeout,
                    "allowed_updates": ["message"],
                }, retries=1)
                if not updates:
                    await asyncio.sleep(1.0)
                    continue

                for upd in updates:
                    self._offset = max(self._offset, upd.get("update_id", 0) + 1)
                    msg = upd.get("message") or {}
                    text = (msg.get("text") or "").strip()
                    chat = str((msg.get("chat") or {}).get("id", ""))
                    if not text.startswith("/"):
                        continue
                    if chat != self.chat_id:
                        log.warning("ignored command from unauthorised chat %s", chat)
                        continue

                    parts = text.split()
                    cmd = parts[0][1:].split("@")[0].lower()
                    args = parts[1:]
                    # Run each command in its own task. If a handler blocks -
                    # on a rate limiter, a hung REST call, anything - polling
                    # must keep running or the bot goes permanently silent.
                    asyncio.create_task(self._dispatch(handler, cmd, args))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("polling error: %s", exc)
                await asyncio.sleep(3.0)


    async def _dispatch(self, handler: CommandHandler, cmd: str, args: List[str]) -> None:
        try:
            reply = await asyncio.wait_for(handler(cmd, args), timeout=self.command_timeout)
        except asyncio.TimeoutError:
            log.warning("command /%s timed out after %ss", cmd, self.command_timeout)
            reply = (f"⏳ <code>/{cmd}</code> timed out after {self.command_timeout}s.\n"
                     f"The engine is busy or an upstream call is stalling - "
                     f"try <code>/health</code>.")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.exception("command /%s failed", cmd)
            reply = f"⚠️ <code>/{cmd}</code> failed: {exc}"
        if reply:
            try:
                await self.send(reply)
            except Exception as exc:  # noqa: BLE001
                log.warning("could not deliver reply to /%s: %s", cmd, exc)


def _split(text: str) -> List[str]:
    if len(text) <= MAX_LEN:
        return [text]
    chunks, cur = [], ""
    for line in text.split("\n"):
        if len(cur) + len(line) + 1 > MAX_LEN:
            chunks.append(cur)
            cur = ""
        cur += line + "\n"
    if cur.strip():
        chunks.append(cur)
    return chunks
