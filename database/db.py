"""
database/db.py
SQLite database layer — the ONLY channel between the master process and the
slave process(es).

The master process writes detected events into `event_queue` and publishes a
snapshot of its open positions/orders into `master_state`. Slave processes read
from those tables, track per-slave delivery in `slave_events` (with atomic
claim semantics so the same event is never executed twice), and record results
in `execution_logs` / `trade_history`.

Everything is durable: a crash/restart of either process loses no events.
"""

from __future__ import annotations
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional
from loguru import logger


def _utcnow() -> datetime:
    """Naive UTC timestamp (replaces deprecated datetime.utcnow())."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Database:
    """Thread-safe SQLite wrapper for the trade copier."""

    def __init__(self, db_path: str = "trade_copier.db"):
        self.db_path = db_path
        self._local = threading.local()
        # Track every per-thread connection so close() can release them all.
        self._all_conns: list[sqlite3.Connection] = []
        self._conns_lock = threading.Lock()
        self._create_tables()
        self._migrate()
        logger.info(f"Database initialised at {db_path}")

    # ── Connection management ──────────────────────────────────────────

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn = conn
            with self._conns_lock:
                self._all_conns.append(conn)
        return self._local.conn

    def close(self):
        """Close every connection opened across all threads."""
        with self._conns_lock:
            for conn in self._all_conns:
                try:
                    conn.close()
                except Exception as exc:
                    logger.warning(f"Error closing DB connection: {exc}")
            self._all_conns.clear()
        self._local = threading.local()
        logger.info("Database connections closed")

    @contextmanager
    def _cursor(self):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    # ── Schema ─────────────────────────────────────────────────────────

    def _create_tables(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS ticket_mapping (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                master_ticket INTEGER NOT NULL,
                slave_login   INTEGER NOT NULL,
                slave_ticket  INTEGER NOT NULL,
                symbol        TEXT    NOT NULL,
                closed        INTEGER NOT NULL DEFAULT 0,
                created_at    TEXT    NOT NULL,
                modified_at   TEXT    NOT NULL,
                closed_at     TEXT,
                UNIQUE(master_ticket, slave_login)
            );

            -- Canonical event log written by the MASTER process.
            CREATE TABLE IF NOT EXISTS event_queue (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id    TEXT    NOT NULL UNIQUE,
                event_type  TEXT    NOT NULL,
                payload     TEXT    NOT NULL,  -- JSON blob (TradeEvent)
                created_at  TEXT    NOT NULL
            );

            -- Per-(event, slave) delivery state used by SLAVE processes.
            CREATE TABLE IF NOT EXISTS slave_events (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id      TEXT    NOT NULL,
                slave_login   INTEGER NOT NULL,
                status        TEXT    NOT NULL DEFAULT 'PENDING',
                attempts      INTEGER NOT NULL DEFAULT 0,
                retcode       INTEGER,
                error         TEXT,
                next_attempt_at TEXT,
                claimed_at    TEXT,
                created_at    TEXT    NOT NULL,
                updated_at    TEXT    NOT NULL,
                UNIQUE(event_id, slave_login)
            );

            -- Per-slave cursor: highest event_queue.id materialised into
            -- slave_events for that slave.
            CREATE TABLE IF NOT EXISTS slave_cursor (
                slave_login INTEGER PRIMARY KEY,
                last_id     INTEGER NOT NULL DEFAULT 0
            );

            -- Latest master snapshot (positions/orders/account) for recovery
            -- and reconciliation. Key is the snapshot section.
            CREATE TABLE IF NOT EXISTS master_state (
                key        TEXT PRIMARY KEY,   -- positions | orders | account
                value      TEXT NOT NULL,      -- JSON blob
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trade_history (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                master_ticket INTEGER NOT NULL,
                slave_login   INTEGER NOT NULL,
                slave_ticket  INTEGER,
                symbol        TEXT    NOT NULL,
                action        TEXT    NOT NULL,
                volume        REAL,
                price         REAL,
                sl            REAL,
                tp            REAL,
                result        TEXT,
                profit        REAL,
                timestamp     TEXT    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS execution_logs (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id      TEXT    NOT NULL,
                slave_login   INTEGER NOT NULL,
                master_ticket INTEGER NOT NULL,
                slave_ticket  INTEGER,
                symbol        TEXT    NOT NULL,
                action        TEXT    NOT NULL,
                status        TEXT    NOT NULL,
                retcode       INTEGER,
                error         TEXT,
                latency_ms    REAL,
                timestamp     TEXT    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS accounts (
                login         INTEGER PRIMARY KEY,
                server        TEXT    NOT NULL,
                role          TEXT    NOT NULL,  -- master | slave
                balance       REAL    DEFAULT 0,
                equity        REAL    DEFAULT 0,
                last_seen     TEXT
            );

            -- Liveness heartbeats per process/component.
            CREATE TABLE IF NOT EXISTS heartbeats (
                component  TEXT PRIMARY KEY,   -- e.g. master, slave:262967799
                status     TEXT NOT NULL,
                detail     TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_ticket_mapping_master
                ON ticket_mapping(master_ticket);
            CREATE INDEX IF NOT EXISTS idx_ticket_mapping_slave
                ON ticket_mapping(slave_login, slave_ticket);
            CREATE INDEX IF NOT EXISTS idx_slave_events_lookup
                ON slave_events(slave_login, status);
            CREATE INDEX IF NOT EXISTS idx_event_queue_id
                ON event_queue(id);
        """)
        conn.commit()
        conn.close()

    def _migrate(self):
        """Add columns to pre-existing databases that lack them."""
        conn = sqlite3.connect(self.db_path)
        try:
            def cols(table):
                return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}

            tm = cols("ticket_mapping")
            if "closed" not in tm:
                conn.execute("ALTER TABLE ticket_mapping ADD COLUMN closed INTEGER NOT NULL DEFAULT 0")
                conn.execute("UPDATE ticket_mapping SET closed=1 WHERE closed_at IS NOT NULL")
            if "modified_at" not in tm:
                conn.execute("ALTER TABLE ticket_mapping ADD COLUMN modified_at TEXT")
                conn.execute("UPDATE ticket_mapping SET modified_at=created_at WHERE modified_at IS NULL")

            el = cols("execution_logs")
            if "retcode" not in el:
                conn.execute("ALTER TABLE execution_logs ADD COLUMN retcode INTEGER")

            ac = cols("accounts")
            if "equity" not in ac:
                conn.execute("ALTER TABLE accounts ADD COLUMN equity REAL DEFAULT 0")

            conn.commit()
        except Exception as exc:
            logger.warning(f"Migration warning: {exc}")
        finally:
            conn.close()

    # ── Ticket Mapping ─────────────────────────────────────────────────

    def save_ticket_mapping(
        self,
        master_ticket: int,
        slave_login: int,
        slave_ticket: int,
        symbol: str,
    ):
        now = _utcnow().isoformat()
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO ticket_mapping
                    (master_ticket, slave_login, slave_ticket, symbol,
                     closed, created_at, modified_at)
                VALUES (?, ?, ?, ?, 0, ?, ?)
                ON CONFLICT(master_ticket, slave_login) DO UPDATE SET
                    slave_ticket = excluded.slave_ticket,
                    symbol       = excluded.symbol,
                    closed       = 0,
                    closed_at    = NULL,
                    modified_at  = excluded.modified_at
            """, (master_ticket, slave_login, slave_ticket, symbol, now, now))
        logger.debug(f"Mapping saved: master={master_ticket} → slave={slave_ticket} ({slave_login})")

    def get_slave_ticket(self, master_ticket: int, slave_login: int) -> Optional[int]:
        with self._cursor() as cur:
            cur.execute("""
                SELECT slave_ticket FROM ticket_mapping
                WHERE master_ticket=? AND slave_login=? AND closed=0
            """, (master_ticket, slave_login))
            row = cur.fetchone()
            return row["slave_ticket"] if row else None

    def get_all_slave_tickets(self, master_ticket: int) -> list[dict]:
        with self._cursor() as cur:
            cur.execute("""
                SELECT slave_login, slave_ticket FROM ticket_mapping
                WHERE master_ticket=? AND closed=0
            """, (master_ticket,))
            return [dict(r) for r in cur.fetchall()]

    def get_open_mappings_for_slave(self, slave_login: int) -> list[dict]:
        with self._cursor() as cur:
            cur.execute("""
                SELECT master_ticket, slave_ticket, symbol FROM ticket_mapping
                WHERE slave_login=? AND closed=0
            """, (slave_login,))
            return [dict(r) for r in cur.fetchall()]

    def mark_mapping_closed(self, master_ticket: int, slave_login: int):
        now = _utcnow().isoformat()
        with self._cursor() as cur:
            cur.execute("""
                UPDATE ticket_mapping SET closed=1, closed_at=?, modified_at=?
                WHERE master_ticket=? AND slave_login=?
            """, (now, now, master_ticket, slave_login))

    # ── Event Queue (master writes) ─────────────────────────────────────

    def enqueue_event(self, event_id: str, event_type: str, payload: str):
        now = _utcnow().isoformat()
        with self._cursor() as cur:
            cur.execute("""
                INSERT OR IGNORE INTO event_queue
                    (event_id, event_type, payload, created_at)
                VALUES (?, ?, ?, ?)
            """, (event_id, event_type, payload, now))

    def event_exists(self, event_id: str) -> bool:
        with self._cursor() as cur:
            cur.execute("SELECT 1 FROM event_queue WHERE event_id=?", (event_id,))
            return cur.fetchone() is not None

    def fetch_events_after(self, after_id: int, limit: int = 500) -> list[dict]:
        with self._cursor() as cur:
            cur.execute("""
                SELECT id, event_id, event_type, payload FROM event_queue
                WHERE id > ? ORDER BY id ASC LIMIT ?
            """, (after_id, limit))
            return [dict(r) for r in cur.fetchall()]

    # ── Per-slave durable queue ─────────────────────────────────────────

    def get_slave_cursor(self, slave_login: int) -> int:
        with self._cursor() as cur:
            cur.execute("SELECT last_id FROM slave_cursor WHERE slave_login=?", (slave_login,))
            row = cur.fetchone()
            return row["last_id"] if row else 0

    def materialise_slave_events(self, slave_login: int, limit: int = 500) -> int:
        """
        Create PENDING slave_events rows for any new master events this slave
        has not yet seen, then advance the slave cursor. Returns how many new
        rows were created.
        """
        cursor = self.get_slave_cursor(slave_login)
        new_events = self.fetch_events_after(cursor, limit=limit)
        if not new_events:
            return 0
        now = _utcnow().isoformat()
        max_id = cursor
        with self._cursor() as cur:
            for ev in new_events:
                cur.execute("""
                    INSERT OR IGNORE INTO slave_events
                        (event_id, slave_login, status, created_at, updated_at, next_attempt_at)
                    VALUES (?, ?, 'PENDING', ?, ?, ?)
                """, (ev["event_id"], slave_login, now, now, now))
                max_id = max(max_id, ev["id"])
            cur.execute("""
                INSERT INTO slave_cursor (slave_login, last_id) VALUES (?, ?)
                ON CONFLICT(slave_login) DO UPDATE SET last_id=excluded.last_id
            """, (slave_login, max_id))
        return len(new_events)

    def get_due_slave_events(self, slave_login: int, limit: int = 100) -> list[dict]:
        """Return PENDING/RETRY events whose backoff has elapsed, oldest first."""
        now = _utcnow().isoformat()
        with self._cursor() as cur:
            cur.execute("""
                SELECT se.event_id, se.attempts, eq.event_type, eq.payload, eq.id AS seq
                FROM slave_events se
                JOIN event_queue eq ON eq.event_id = se.event_id
                WHERE se.slave_login = ?
                  AND se.status IN ('PENDING', 'RETRY')
                  AND (se.next_attempt_at IS NULL OR se.next_attempt_at <= ?)
                ORDER BY eq.id ASC
                LIMIT ?
            """, (slave_login, now, limit))
            return [dict(r) for r in cur.fetchall()]

    def claim_slave_event(self, event_id: str, slave_login: int) -> bool:
        """
        Atomically transition PENDING/RETRY → PROCESSING. Returns True only if
        THIS caller won the claim (rowcount==1), preventing double execution
        across threads/processes.
        """
        now = _utcnow().isoformat()
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                UPDATE slave_events
                SET status='PROCESSING', claimed_at=?, updated_at=?
                WHERE event_id=? AND slave_login=? AND status IN ('PENDING','RETRY')
            """, (now, now, event_id, slave_login))
            conn.commit()
            return cur.rowcount == 1
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    def complete_slave_event(
        self, event_id: str, slave_login: int,
        status: str = "COMPLETED", retcode: Optional[int] = None,
        error: Optional[str] = None,
    ):
        now = _utcnow().isoformat()
        with self._cursor() as cur:
            cur.execute("""
                UPDATE slave_events
                SET status=?, retcode=?, error=?, updated_at=?,
                    attempts=attempts+1
                WHERE event_id=? AND slave_login=?
            """, (status, retcode, error, now, event_id, slave_login))

    def retry_slave_event(
        self, event_id: str, slave_login: int, delay_ms: int,
        max_retries: int, retcode: Optional[int] = None, error: Optional[str] = None,
    ):
        """Bump attempts; schedule RETRY with backoff or give up → FAILED."""
        now = _utcnow()
        with self._cursor() as cur:
            cur.execute("""
                SELECT attempts FROM slave_events WHERE event_id=? AND slave_login=?
            """, (event_id, slave_login))
            row = cur.fetchone()
            attempts = (row["attempts"] if row else 0) + 1
            if attempts >= max_retries:
                cur.execute("""
                    UPDATE slave_events
                    SET status='FAILED', attempts=?, retcode=?, error=?, updated_at=?
                    WHERE event_id=? AND slave_login=?
                """, (attempts, retcode, error, now.isoformat(), event_id, slave_login))
            else:
                next_at = (now + timedelta(milliseconds=delay_ms * attempts)).isoformat()
                cur.execute("""
                    UPDATE slave_events
                    SET status='RETRY', attempts=?, retcode=?, error=?,
                        next_attempt_at=?, updated_at=?
                    WHERE event_id=? AND slave_login=?
                """, (attempts, retcode, error, next_at, now.isoformat(),
                      event_id, slave_login))

    def recover_stuck_slave_events(self, slave_login: int, stale_seconds: int = 60) -> int:
        """
        On startup (or periodically), reset PROCESSING rows that were claimed
        but never finished (process crashed mid-execution) back to RETRY.
        """
        cutoff = (_utcnow() - timedelta(seconds=stale_seconds)).isoformat()
        with self._cursor() as cur:
            cur.execute("""
                UPDATE slave_events
                SET status='RETRY', updated_at=?
                WHERE slave_login=? AND status='PROCESSING'
                  AND (claimed_at IS NULL OR claimed_at <= ?)
            """, (_utcnow().isoformat(), slave_login, cutoff))
            return cur.rowcount

    # ── Master snapshot (for recovery / reconciliation) ─────────────────

    def save_master_state(self, key: str, value_json: str):
        now = _utcnow().isoformat()
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO master_state (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value, updated_at=excluded.updated_at
            """, (key, value_json, now))

    def get_master_state(self, key: str) -> Optional[str]:
        with self._cursor() as cur:
            cur.execute("SELECT value FROM master_state WHERE key=?", (key,))
            row = cur.fetchone()
            return row["value"] if row else None

    # ── Execution Logs ─────────────────────────────────────────────────

    def log_execution(
        self,
        event_id: str,
        slave_login: int,
        master_ticket: int,
        symbol: str,
        action: str,
        status: str,
        slave_ticket: Optional[int] = None,
        error: Optional[str] = None,
        latency_ms: Optional[float] = None,
        retcode: Optional[int] = None,
    ):
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO execution_logs
                    (event_id, slave_login, master_ticket, slave_ticket, symbol,
                     action, status, retcode, error, latency_ms, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (event_id, slave_login, master_ticket, slave_ticket, symbol,
                  action, status, retcode, error, latency_ms,
                  _utcnow().isoformat()))

    # ── Trade History ──────────────────────────────────────────────────

    def record_trade(
        self,
        master_ticket: int,
        slave_login: int,
        symbol: str,
        action: str,
        slave_ticket: Optional[int] = None,
        volume: Optional[float] = None,
        price: Optional[float] = None,
        sl: Optional[float] = None,
        tp: Optional[float] = None,
        result: Optional[str] = None,
    ):
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO trade_history
                    (master_ticket, slave_login, slave_ticket, symbol, action,
                     volume, price, sl, tp, result, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (master_ticket, slave_login, slave_ticket, symbol, action,
                  volume, price, sl, tp, result,
                  _utcnow().isoformat()))

    # ── Accounts & heartbeats ───────────────────────────────────────────

    def upsert_account(self, login: int, server: str, role: str,
                       balance: float = 0, equity: float = 0):
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO accounts (login, server, role, balance, equity, last_seen)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(login) DO UPDATE SET
                    balance   = excluded.balance,
                    equity    = excluded.equity,
                    server    = excluded.server,
                    last_seen = excluded.last_seen
            """, (login, server, role, balance, equity, _utcnow().isoformat()))

    def heartbeat(self, component: str, status: str = "ALIVE", detail: str = ""):
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO heartbeats (component, status, detail, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(component) DO UPDATE SET
                    status=excluded.status, detail=excluded.detail,
                    updated_at=excluded.updated_at
            """, (component, status, detail, _utcnow().isoformat()))

    def get_heartbeats(self) -> list[dict]:
        with self._cursor() as cur:
            cur.execute("SELECT * FROM heartbeats ORDER BY component")
            return [dict(r) for r in cur.fetchall()]

    def get_accounts(self) -> list[dict]:
        with self._cursor() as cur:
            cur.execute("SELECT * FROM accounts ORDER BY role DESC, login")
            return [dict(r) for r in cur.fetchall()]

    def get_recent_executions(self, limit: int = 50) -> list[dict]:
        with self._cursor() as cur:
            cur.execute("""
                SELECT * FROM execution_logs
                ORDER BY id DESC LIMIT ?
            """, (limit,))
            return [dict(r) for r in cur.fetchall()]

    def get_queue_depth(self) -> dict:
        with self._cursor() as cur:
            cur.execute("""
                SELECT status, COUNT(*) AS n FROM slave_events GROUP BY status
            """)
            return {r["status"]: r["n"] for r in cur.fetchall()}

    def get_stats(self) -> dict:
        with self._cursor() as cur:
            cur.execute("SELECT COUNT(*) as total FROM ticket_mapping WHERE closed=0")
            open_trades = cur.fetchone()["total"]
            cur.execute("SELECT COUNT(*) as total FROM ticket_mapping WHERE closed=1")
            closed_trades = cur.fetchone()["total"]
            cur.execute("SELECT COUNT(*) as total FROM execution_logs WHERE status='FAILED'")
            failed = cur.fetchone()["total"]
            cur.execute("SELECT COUNT(*) as total FROM execution_logs WHERE status='SUCCESS'")
            success = cur.fetchone()["total"]
        return {
            "open_trades": open_trades,
            "closed_trades": closed_trades,
            "failed_executions": failed,
            "successful_executions": success,
        }
