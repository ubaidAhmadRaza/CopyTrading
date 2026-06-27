"""
core/monitor.py
MT5 Monitor — runs inside the MASTER process. Polls the master account on a
configurable interval, diffs against the previous snapshot, and writes detected
TradeEvents into the durable SQLite event_queue (no in-memory queue).

Each event carries a snapshot of the master balance/equity so that slave
processes can size lots relative to the master WITHOUT ever connecting to the
master terminal. The monitor also publishes the full master position/order set
to `master_state` for slave-side recovery and reconciliation.
"""

from __future__ import annotations
import json
import threading
import time
from loguru import logger

from brokers.mt5_connection import MT5Connection
from core.state_manager import StateManager, parse_position, parse_order
from database.db import Database


class MT5Monitor:
    """
    Runs a polling loop in its own thread. Detected events are persisted to the
    database; the master snapshot is republished every poll.
    """

    def __init__(
        self,
        master_conn: MT5Connection,
        state_manager: StateManager,
        db: Database,
        poll_interval_ms: int = 250,
        heartbeat_interval_s: int = 5,
    ):
        self.master_conn = master_conn
        self.state_manager = state_manager
        self.db = db
        self.poll_interval = poll_interval_ms / 1000.0
        self.heartbeat_interval = heartbeat_interval_s

        self._running = False
        self._thread: threading.Thread | None = None
        self._consecutive_errors = 0
        self._max_consecutive_errors = 10
        self._last_heartbeat = 0.0

    # ── Lifecycle ──────────────────────────────────────────────────────

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name="MT5Monitor")
        self._thread.start()
        logger.info("MT5Monitor started")

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("MT5Monitor stopped")

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # ── Poll loop ──────────────────────────────────────────────────────

    def _loop(self):
        while self._running:
            t0 = time.monotonic()
            try:
                self._poll()
                self._consecutive_errors = 0
            except Exception as exc:
                self._consecutive_errors += 1
                logger.error(f"Monitor poll error ({self._consecutive_errors}): {exc}")
                if self._consecutive_errors >= self._max_consecutive_errors:
                    logger.critical("Too many consecutive monitor errors — resetting state")
                    self.state_manager.reset()
                    self._consecutive_errors = 0

            elapsed = time.monotonic() - t0
            time.sleep(max(0, self.poll_interval - elapsed))

    def _poll(self):
        if not self.master_conn.ensure_connected():
            logger.warning("Master disconnected — waiting for reconnect")
            self.db.heartbeat("master", "DISCONNECTED", "reconnecting")
            time.sleep(2)
            return

        acct = self.master_conn.account_info()
        balance = float(getattr(acct, "balance", 0.0)) if acct else 0.0
        equity = float(getattr(acct, "equity", 0.0)) if acct else 0.0

        raw_positions = self.master_conn.positions_get()
        raw_orders = self.master_conn.orders_get()

        positions = [parse_position(p) for p in raw_positions]
        orders = [parse_order(o) for o in raw_orders]

        # Publish snapshot first so a freshly-started slave can reconcile even
        # before the next event arrives.
        self._publish_snapshot(positions, orders, balance, equity)

        events = self.state_manager.compare(positions, orders)

        for event in events:
            # Stamp master account state onto the event for balance-relative
            # sizing on the slave side.
            event.master_balance = balance
            event.master_equity = equity
            self.db.enqueue_event(
                event_id=event.event_id,
                event_type=event.event_type.value,
                payload=event.model_dump_json(),
            )
            logger.info(
                f"Event queued: {event.event_type.value} | "
                f"ticket={event.master_ticket} | {event.symbol}"
            )

        # Periodic heartbeat + account refresh.
        now = time.monotonic()
        if now - self._last_heartbeat >= self.heartbeat_interval:
            self._last_heartbeat = now
            if acct:
                self.db.upsert_account(acct.login, self.master_conn.server,
                                       "master", balance, equity)
            self.db.heartbeat(
                "master", "ALIVE",
                f"pos={len(positions)} ord={len(orders)} bal={balance:.2f}",
            )

    def _publish_snapshot(self, positions, orders, balance, equity):
        self.db.save_master_state(
            "positions",
            json.dumps([p.model_dump(mode="json") for p in positions]),
        )
        self.db.save_master_state(
            "orders",
            json.dumps([o.model_dump(mode="json") for o in orders]),
        )
        self.db.save_master_state(
            "account",
            json.dumps({"balance": balance, "equity": equity}),
        )
