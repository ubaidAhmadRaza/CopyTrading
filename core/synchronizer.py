"""
core/synchronizer.py
Synchronizer — runs inside the SLAVE process. Pulls events from the durable
SQLite queue (written by the master process), claims them atomically, executes
them on each local slave account, and drives the per-event retry lifecycle.

It also runs a periodic reconciliation pass that compares the published master
snapshot against the slave's open positions to detect & repair drift:
missed opens, stale closes, etc.
"""

from __future__ import annotations
import json
import threading
import time
from loguru import logger

from core.executor import Executor
from database.db import Database
from models.schemas import TradeEvent, EventType


class Synchronizer:
    """Consumes the durable queue and applies events to local slave executors."""

    def __init__(
        self,
        executors: list[Executor],
        db: Database,
        sync_interval_ms: int = 100,
        reconcile_interval_s: int = 30,
        max_retries: int = 3,
        retry_delay_ms: int = 500,
        heartbeat_interval_s: int = 5,
    ):
        self.executors          = executors
        self.db                 = db
        self.sync_interval      = sync_interval_ms / 1000.0
        self.reconcile_interval = reconcile_interval_s
        self.max_retries        = max_retries
        self.retry_delay_ms     = retry_delay_ms
        self.heartbeat_interval = heartbeat_interval_s

        self._running         = False
        self._thread: threading.Thread | None = None
        self._last_reconcile  = 0.0
        self._last_heartbeat  = 0.0

    # ── Lifecycle ──────────────────────────────────────────────────────

    def start(self):
        if self._running:
            return
        self._running = True
        # On startup requeue any events left PROCESSING by a previous crash.
        for ex in self.executors:
            recovered = self.db.recover_stuck_slave_events(ex.config.login)
            if recovered:
                logger.warning(f"[{ex.config.login}] Recovered {recovered} stuck event(s)")
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="Synchronizer"
        )
        self._thread.start()
        logger.info("Synchronizer started")

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Synchronizer stopped")

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # ── Processing loop ────────────────────────────────────────────────

    def _loop(self):
        while self._running:
            try:
                worked = self._drain_once()
            except Exception as exc:
                logger.exception(f"Synchronizer loop error: {exc}")
                worked = False

            now = time.monotonic()
            if now - self._last_reconcile >= self.reconcile_interval:
                self._last_reconcile = now
                try:
                    self._reconcile()
                except Exception as exc:
                    logger.exception(f"Reconciliation error: {exc}")

            if now - self._last_heartbeat >= self.heartbeat_interval:
                self._last_heartbeat = now
                self._emit_heartbeats()

            if not worked:
                time.sleep(self.sync_interval)

    def _drain_once(self) -> bool:
        """Process one batch of due events per slave. Returns True if any ran."""
        did_work = False
        for ex in self.executors:
            login = ex.config.login
            self.db.materialise_slave_events(login)
            due = self.db.get_due_slave_events(login, limit=50)
            for row in due:
                if not self.db.claim_slave_event(row["event_id"], login):
                    continue
                did_work = True
                self._run_event(ex, row)
        return did_work

    def _run_event(self, ex: Executor, row: dict):
        login = ex.config.login
        try:
            event = TradeEvent.model_validate_json(row["payload"])
        except Exception as exc:
            logger.error(f"[{login}] Bad event payload {row['event_id']}: {exc}")
            self.db.complete_slave_event(
                row["event_id"], login, "FAILED", error="bad payload"
            )
            return

        result = ex.execute(event)

        if result.ok:
            status = "SKIPPED" if result.skipped else "COMPLETED"
            self.db.complete_slave_event(
                row["event_id"], login, status,
                retcode=result.retcode, error=result.error,
            )
            logger.debug(f"[{login}] {event.event_type.value} → {status}")
        else:
            self.db.retry_slave_event(
                row["event_id"], login,
                delay_ms=self.retry_delay_ms, max_retries=self.max_retries,
                retcode=result.retcode, error=result.error,
            )
            logger.warning(
                f"[{login}] {event.event_type.value} failed "
                f"(attempt {row['attempts'] + 1}/{self.max_retries}): {result.error}"
            )

    # ── Reconciliation & missed-event detection ────────────────────────

    def _reconcile(self):
        """
        Compare the master snapshot to each slave's open positions:
          * master position with no slave mapping → synthesize a NEW_POSITION
            event so the missing trade gets copied.
          * slave mapping whose master position is gone → synthesize a
            CLOSE_POSITION event.
        Synthesized events flow through the normal durable queue, so they are
        deduped and retried like any other event.
        """
        snap     = self.db.get_master_state("positions")
        acct_raw = self.db.get_master_state("account")
        if snap is None:
            return
        try:
            master_positions = json.loads(snap)
            acct             = json.loads(acct_raw) if acct_raw else {}
        except Exception:
            return

        master_by_ticket = {int(p["ticket"]): p for p in master_positions}
        master_balance   = float(acct.get("balance", 0.0))
        master_equity    = float(acct.get("equity",  0.0))

        for ex in self.executors:
            login  = ex.config.login
            mapped = {m["master_ticket"] for m in self.db.get_open_mappings_for_slave(login)}

            # Missed opens: on master, not yet mapped on slave.
            for ticket, p in master_by_ticket.items():
                if ticket not in mapped:
                    if self._has_pending_open(ticket, login):
                        continue
                    logger.warning(
                        f"[{login}] Reconcile: missed OPEN for master={ticket} — enqueuing"
                    )
                    self._enqueue_synthetic(
                        EventType.NEW_POSITION, p, master_balance, master_equity
                    )

            # Stale closes: mapped on slave, gone on master.
            for ticket in mapped:
                if ticket not in master_by_ticket:
                    logger.warning(
                        f"[{login}] Reconcile: stale CLOSE for master={ticket} — enqueuing"
                    )
                    self._enqueue_synthetic_close(ticket, master_balance, master_equity)

    def _has_pending_open(self, master_ticket: int, login: int) -> bool:
        """
        Return True if there is already a non-terminal NEW_POSITION event in
        the queue for this master_ticket / slave pair.

        BUG 4 FIX: The original implementation only called
        get_due_slave_events() which filters by next_attempt_at <= now.
        Events in RETRY with a future next_attempt_at were invisible, causing
        the reconciler to enqueue a duplicate synthetic open on every
        reconcile cycle until the backoff elapsed and the first event ran.

        We now query slave_events directly for any row in a non-terminal status
        (PENDING, RETRY, PROCESSING) regardless of next_attempt_at, then check
        the event payload to confirm the event type.
        """
        in_flight = self.db.get_in_flight_slave_events(login)
        for row in in_flight:
            try:
                ev = TradeEvent.model_validate_json(row["payload"])
            except Exception:
                continue
            if (
                ev.master_ticket == master_ticket
                and ev.event_type == EventType.NEW_POSITION
            ):
                return True
        return False

    def _enqueue_synthetic(self, event_type, p, balance, equity):
        import uuid
        event = TradeEvent(
            event_id=f"recon-{event_type.value}-{p['ticket']}-{uuid.uuid4().hex[:8]}",
            event_type=event_type,
            master_ticket=int(p["ticket"]),
            symbol=p["symbol"],
            trade_type=p.get("trade_type"),
            volume=p.get("volume"),
            price=p.get("open_price"),
            sl=p.get("sl"),
            tp=p.get("tp"),
            master_balance=balance,
            master_equity=equity,
        )
        self.db.enqueue_event(
            event.event_id, event.event_type.value, event.model_dump_json()
        )

    def _enqueue_synthetic_close(self, master_ticket, balance, equity):
        import uuid
        event = TradeEvent(
            event_id=f"recon-CLOSE-{master_ticket}-{uuid.uuid4().hex[:8]}",
            event_type=EventType.CLOSE_POSITION,
            master_ticket=master_ticket,
            symbol="",
            master_balance=balance,
            master_equity=equity,
        )
        self.db.enqueue_event(
            event.event_id, event.event_type.value, event.model_dump_json()
        )

    # ── Heartbeats ─────────────────────────────────────────────────────

    def _emit_heartbeats(self):
        for ex in self.executors:
            login     = ex.config.login
            connected = ex.conn.is_connected()
            acct      = ex.conn.account_info() if connected else None
            bal       = float(getattr(acct, "balance", 0.0)) if acct else 0.0
            eq        = float(getattr(acct, "equity",  0.0)) if acct else 0.0
            if acct:
                self.db.upsert_account(login, ex.conn.server, "slave", bal, eq)
            self.db.heartbeat(
                f"slave:{login}",
                "ALIVE" if connected else "DISCONNECTED",
                f"bal={bal:.2f} eq={eq:.2f}",
            )