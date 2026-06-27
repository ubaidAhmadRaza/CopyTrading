"""
services/reliability.py
Reliability helpers shared by the master and slave processes:
graceful shutdown handling and a watchdog that restarts dead worker threads.
"""

from __future__ import annotations
import signal
import threading
import time
from typing import Callable
from loguru import logger


class ShutdownHandler:
    """Installs SIGINT/SIGTERM handlers and exposes a wait()-able event."""

    def __init__(self):
        self._event = threading.Event()
        self._callbacks: list[Callable[[], None]] = []

    def install(self):
        signal.signal(signal.SIGINT, self._handle)
        try:
            signal.signal(signal.SIGTERM, self._handle)
        except (ValueError, AttributeError):
            # SIGTERM may be unavailable on some Windows setups.
            pass

    def on_shutdown(self, cb: Callable[[], None]):
        self._callbacks.append(cb)

    def _handle(self, sig, frame):
        logger.info(f"Shutdown signal ({sig}) received…")
        self.trigger()

    def trigger(self):
        if self._event.is_set():
            return
        self._event.set()
        for cb in self._callbacks:
            try:
                cb()
            except Exception as exc:
                logger.warning(f"Shutdown callback error: {exc}")

    @property
    def triggered(self) -> bool:
        return self._event.is_set()

    def wait(self, timeout: float | None = None) -> bool:
        return self._event.wait(timeout)


class Watchdog:
    """
    Periodically checks that registered components report alive; if a component
    is dead it calls its restart hook. Runs in its own daemon thread.
    """

    def __init__(self, interval_s: float = 5.0):
        self.interval_s = interval_s
        self._checks: list[tuple[str, Callable[[], bool], Callable[[], None]]] = []
        self._running = False
        self._thread: threading.Thread | None = None

    def register(self, name: str, is_alive: Callable[[], bool], restart: Callable[[], None]):
        self._checks.append((name, is_alive, restart))

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name="Watchdog")
        self._thread.start()
        logger.info("Watchdog started")

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=3)

    def _loop(self):
        while self._running:
            time.sleep(self.interval_s)
            if not self._running:
                break
            for name, is_alive, restart in self._checks:
                try:
                    if not is_alive():
                        logger.error(f"Watchdog: '{name}' is DOWN — restarting")
                        restart()
                except Exception as exc:
                    logger.exception(f"Watchdog check '{name}' raised: {exc}")
