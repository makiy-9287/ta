#!/usr/bin/env python3
"""
Sniper Flow - order-flow signal engine for Binance USDT-M Futures.

    python main.py              start the engine
    python main.py --selftest   run the offline logic tests (no network, no keys)
    python main.py --check      validate configuration and exit

Signals and alerts only. This program never places an order and never needs
an API key - only a Telegram bot token and chat id.
"""
from __future__ import annotations

import argparse
import asyncio
import signal
import sys

from config import SETTINGS
from core.utils import get_logger, setup_logging

log = get_logger("main")

BANNER = r"""
  ___ _  _ ___ ___ ___ ___   ___ _    _____      __
 / __| \| |_ _| _ \ __| _ \ | __| |  / _ \ \    / /
 \__ \ .` || ||  _/ _||   / | _|| |_| (_) \ \/\/ /
 |___/_|\_|___|_| |___|_|_\ |_| |____\___/ \_/\_/
      order flow · absorption · liquidity · S/R
"""


async def _run() -> int:
    from core.engine import SniperEngine

    engine = SniperEngine(SETTINGS)
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        log.info("stop signal received")
        engine._stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:  # windows
            pass

    try:
        await engine.run_forever()
    except KeyboardInterrupt:
        pass
    except Exception as exc:  # noqa: BLE001
        log.exception("fatal error: %s", exc)
        try:
            await engine.bot.send(f"💥 <b>Engine crashed</b>\n<code>{exc}</code>")
        except Exception:
            pass
        return 1
    finally:
        await engine.shutdown()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Sniper Flow signal engine")
    parser.add_argument("--selftest", action="store_true", help="run offline logic tests")
    parser.add_argument("--check", action="store_true", help="validate config and exit")
    args = parser.parse_args()

    setup_logging(SETTINGS.log_level, SETTINGS.log_file)

    if args.selftest:
        from selftest import run_selftest
        return 0 if run_selftest() else 1

    print(BANNER)
    problems = SETTINGS.validate()
    if problems:
        for p in problems:
            log.error("config: %s", p)
        log.error("copy .env.example to .env and fill in the two Telegram values")
        return 2

    if args.check:
        log.info("configuration looks good")
        for k, v in SETTINGS.as_dict().items():
            log.info("  %-26s %s", k, v)
        return 0

    return asyncio.run(_run())


if __name__ == "__main__":
    sys.exit(main())
