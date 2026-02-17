import asyncio
import logging
import time
from dataclasses import dataclass, field

from aiogram import Bot

import config

logger = logging.getLogger(__name__)

PRIORITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3}


@dataclass(order=True)
class Message:
    priority: int
    timestamp: float = field(compare=False)
    text: str = field(compare=False)
    msg_type: str = field(compare=False, default="")


class Notifier:
    def __init__(self, bot: Bot):
        self._bot = bot
        self._queue: asyncio.PriorityQueue[Message] = asyncio.PriorityQueue(
            maxsize=config.NOTIFIER_MAX_QUEUE
        )
        self._running = False
        self._task: asyncio.Task | None = None
        self._recent: list[Message] = []

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._worker())
        logger.info("Notifier worker started")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        # drain queue
        while not self._queue.empty():
            msg = self._queue.get_nowait()
            await self._send_message(msg.text)
            await asyncio.sleep(config.NOTIFIER_DELAY)
        logger.info("Notifier stopped")

    async def send(
        self, text: str, priority: str = "high", msg_type: str = ""
    ) -> None:
        pri = PRIORITY_ORDER.get(priority, 2)
        msg = Message(
            priority=pri, timestamp=time.time(), text=text, msg_type=msg_type
        )
        if self._queue.full():
            # drop low priority
            logger.warning("Notifier queue full, dropping message")
            return
        await self._queue.put(msg)

    async def _worker(self) -> None:
        while self._running:
            try:
                msg = await asyncio.wait_for(
                    self._queue.get(), timeout=5.0
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            self._recent.append(msg)
            now = time.time()
            self._recent = [
                m for m in self._recent
                if now - m.timestamp < config.MASS_ALERT_WINDOW
            ]

            # check for mass alerts
            if msg.msg_type:
                same_type = [
                    m for m in self._recent if m.msg_type == msg.msg_type
                ]
                if len(same_type) > config.MASS_ALERT_THRESHOLD:
                    grouped = self._group_mass_alert(same_type)
                    await self._send_message(grouped)
                    self._recent = [
                        m for m in self._recent
                        if m.msg_type != msg.msg_type
                    ]
                    await asyncio.sleep(config.NOTIFIER_DELAY)
                    continue

            await self._send_message(msg.text)
            await asyncio.sleep(config.NOTIFIER_DELAY)

    def _group_mass_alert(self, messages: list[Message]) -> str:
        symbols = []
        for m in messages:
            parts = m.text.split()
            for p in parts:
                if p.endswith("USDT") or p.endswith("USDT:"):
                    symbols.append(p.rstrip(":"))
                    break
        symbols_str = ", ".join(symbols[:6])
        if len(symbols) > 6:
            symbols_str += "..."
        atype = messages[0].msg_type if messages else "anomaly"
        return (
            f"\U0001f534 MASS ALERT: {len(messages)} pairs with {atype}\n"
            f"{symbols_str}\n"
            f"\u26a0\ufe0f Market-wide event"
        )

    async def _send_message(self, text: str) -> None:
        if not config.ADMIN_ID:
            logger.debug("No ADMIN_ID, skipping notification")
            return
        try:
            await self._bot.send_message(
                config.ADMIN_ID, text, parse_mode="HTML"
            )
        except Exception as exc:
            err_str = str(exc).lower()
            if "retry after" in err_str or "429" in err_str:
                retry = 5
                try:
                    for part in str(exc).split():
                        if part.isdigit():
                            retry = int(part)
                            break
                except ValueError:
                    pass
                logger.warning("Telegram 429, waiting %ds", retry)
                await asyncio.sleep(retry)
                try:
                    await self._bot.send_message(
                        config.ADMIN_ID, text, parse_mode="HTML"
                    )
                except Exception:
                    logger.error("Retry send failed")
            else:
                logger.error("Telegram send error: %s", exc)
