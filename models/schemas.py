"""
models/schemas.py
Pydantic models for all data structures used across the trade copier.
"""

from __future__ import annotations
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field
from datetime import datetime, timezone


def _utcnow() -> datetime:
    """Naive UTC timestamp (replaces deprecated datetime.utcnow())."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ─────────────────────────────────────────────
# Enumerations
# ─────────────────────────────────────────────

class Role(str, Enum):
    MASTER = "master"
    SLAVE = "slave"


class TradeType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    BUY_LIMIT = "BUY_LIMIT"
    SELL_LIMIT = "SELL_LIMIT"
    BUY_STOP = "BUY_STOP"
    SELL_STOP = "SELL_STOP"


class EventType(str, Enum):
    NEW_POSITION = "NEW_POSITION"
    MODIFY_POSITION = "MODIFY_POSITION"
    PARTIAL_CLOSE = "PARTIAL_CLOSE"
    CLOSE_POSITION = "CLOSE_POSITION"
    NEW_PENDING = "NEW_PENDING"
    MODIFY_PENDING = "MODIFY_PENDING"
    DELETE_PENDING = "DELETE_PENDING"


class LotMode(str, Enum):
    FIXED = "fixed"
    MULTIPLIER = "multiplier"
    RATIO = "ratio"
    RISK_PERCENT = "risk_percent"
    EXACT = "exact"


class CopyMode(str, Enum):
    COPY = "copy"
    REVERSE = "reverse"


class ExecutionStatus(str, Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    SKIPPED = "SKIPPED"


class EventStatus(str, Enum):
    """Lifecycle of an event in the durable queue (per-slave delivery)."""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRY = "RETRY"
    SKIPPED = "SKIPPED"


# ─────────────────────────────────────────────
# Trade Structures
# ─────────────────────────────────────────────

class Position(BaseModel):
    ticket: int
    symbol: str
    trade_type: TradeType
    volume: float
    open_price: float
    sl: float = 0.0
    tp: float = 0.0
    profit: float = 0.0
    open_time: datetime
    comment: str = ""
    magic: int = 0


class PendingOrder(BaseModel):
    ticket: int
    symbol: str
    trade_type: TradeType
    volume: float
    price: float
    sl: float = 0.0
    tp: float = 0.0
    expiration: Optional[datetime] = None
    comment: str = ""
    magic: int = 0


# ─────────────────────────────────────────────
# Events
# ─────────────────────────────────────────────

class TradeEvent(BaseModel):
    event_id: str
    event_type: EventType
    master_ticket: int
    symbol: str
    trade_type: Optional[TradeType] = None
    volume: Optional[float] = None
    price: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    close_volume: Optional[float] = None   # for partial close
    # Master account snapshot captured at detection time. The executor must
    # NEVER talk to the master MT5 terminal — it relies on these values for
    # balance-relative lot sizing (P2 / TODO #11).
    master_balance: float = 0.0
    master_equity: float = 0.0
    master_volume: Optional[float] = None  # remaining master volume after change
    timestamp: datetime = Field(default_factory=_utcnow)
    processed: bool = False


# ─────────────────────────────────────────────
# Risk Management
# ─────────────────────────────────────────────

class RiskConfig(BaseModel):
    """Per-slave risk guardrails (P7)."""
    max_spread_points: float = 0.0        # 0 = disabled
    max_slippage_points: int = 20
    max_lot: float = 100.0
    max_daily_loss: float = 0.0           # 0 = disabled, account currency
    allowed_symbols: list[str] = []       # empty = all allowed
    blacklist_symbols: list[str] = []
    trading_sessions: list[str] = []      # e.g. ["08:00-17:00"] UTC, empty = 24h
    check_margin: bool = True


# ─────────────────────────────────────────────
# Account Configuration
# ─────────────────────────────────────────────

class AccountConfig(BaseModel):
    login: int
    password: str
    server: str
    terminal_path: Optional[str] = None
    mode: CopyMode = CopyMode.COPY
    lot_mode: LotMode = LotMode.RATIO
    lot_value: float = 1.0
    max_lot: float = 10.0
    min_lot: float = 0.01
    enabled: bool = True
    risk: RiskConfig = Field(default_factory=RiskConfig)


class MasterConfig(BaseModel):
    login: int
    password: str
    server: str
    terminal_path: Optional[str] = None


class AppConfig(BaseModel):
    role: Role
    master: Optional[MasterConfig] = None
    slaves: list[AccountConfig] = []
    symbol_mapping: dict[str, str] = {}
    poll_interval_ms: int = 250
    sync_interval_ms: int = 100
    reconcile_interval_s: int = 30
    heartbeat_interval_s: int = 5
    max_retries: int = 3
    retry_delay_ms: int = 500
    reconnect_max_wait_s: int = 60
    db_path: str = "trade_copier.db"
    log_level: str = "INFO"
    enable_dashboard: bool = True


# ─────────────────────────────────────────────
# Execution Records
# ─────────────────────────────────────────────

class ExecutionRecord(BaseModel):
    event_id: str
    slave_login: int
    master_ticket: int
    slave_ticket: Optional[int] = None
    symbol: str
    action: EventType
    status: ExecutionStatus
    error: Optional[str] = None
    latency_ms: Optional[float] = None
    retcode: Optional[int] = None
    timestamp: datetime = Field(default_factory=_utcnow)


class TicketMapping(BaseModel):
    master_ticket: int
    slave_login: int
    slave_ticket: int
    symbol: str
    closed: bool = False
    created_at: datetime = Field(default_factory=_utcnow)
    modified_at: datetime = Field(default_factory=_utcnow)
