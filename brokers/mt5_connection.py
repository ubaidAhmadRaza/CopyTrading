"""
brokers/mt5_connection.py
MT5 connection wrapper with automatic reconnection and exponential backoff.

CRITICAL ARCHITECTURE RULE
--------------------------
The MetaTrader5 Python package supports exactly ONE active terminal connection
per process. Therefore:

  * Each process creates AT MOST ONE MT5Connection.
  * connect() initialises the terminal at a specific `terminal_path` and logs
    into a specific account.
  * Reconnection ALWAYS targets the SAME terminal + SAME login. We never switch
    accounts inside a live process — that was the root cause of the
    disconnect / reconnect loops.
"""

from __future__ import annotations
import time
import threading
from typing import Optional
from loguru import logger

try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False
    logger.warning("MetaTrader5 package not installed — running in MOCK mode")


class MT5Connection:
    """
    Wraps a single MT5 login session bound to a single terminal path.
    Handles connect / disconnect / reconnect transparently — always to the
    same account.
    """

    def __init__(
        self,
        login: int,
        password: str,
        server: str,
        terminal_path: Optional[str] = None,
        label: str = "",
        max_wait_s: int = 60,
    ):
        self.login = login
        self.password = password
        self.server = server
        self.terminal_path = terminal_path
        self.label = label or str(login)
        self.max_wait_s = max_wait_s

        self._connected = False
        self._lock = threading.Lock()

    # ── Public API ─────────────────────────────────────────────────────

    def connect(self) -> bool:
        """Establish MT5 connection. Returns True on success."""
        with self._lock:
            return self._connect_inner()

    def disconnect(self):
        if not MT5_AVAILABLE:
            self._connected = False
            return
        mt5.shutdown()
        self._connected = False
        logger.info(f"[{self.label}] MT5 disconnected")

    def is_connected(self) -> bool:
        if not MT5_AVAILABLE:
            return self._connected
        try:
            info = mt5.terminal_info()
            if info is None or not info.connected:
                return False
            # Verify we are still logged into the EXPECTED account. If the
            # terminal silently switched accounts, treat as disconnected so a
            # reconnect re-binds the right login.
            acct = mt5.account_info()
            return acct is not None and acct.login == self.login
        except Exception:
            return False

    def ensure_connected(self) -> bool:
        """Reconnect if necessary, with exponential backoff."""
        if self.is_connected():
            return True
        logger.warning(f"[{self.label}] Connection lost — reconnecting…")
        with self._lock:
            return self._reconnect_with_backoff()

    # ── Internal helpers ───────────────────────────────────────────────

    def _connect_inner(self) -> bool:
        if not MT5_AVAILABLE:
            logger.info(f"[{self.label}] MOCK connect (MT5 not installed)")
            self._connected = True
            return True

        # Initialise the SPECIFIC terminal binary and log in atomically.
        # Passing login/password/server to initialize() ensures the terminal
        # is bound to the intended account from the very first call.
        init_kwargs = {
            "login": self.login,
            "password": self.password,
            "server": self.server,
        }
        if self.terminal_path:
            init_kwargs["path"] = self.terminal_path

        if not mt5.initialize(**init_kwargs):
            logger.error(
                f"[{self.label}] mt5.initialize(path={self.terminal_path}) "
                f"failed: {mt5.last_error()}"
            )
            return False

        info = mt5.account_info()
        if info is None or info.login != self.login:
            logger.error(
                f"[{self.label}] Logged into unexpected account "
                f"({getattr(info, 'login', None)} != {self.login})"
            )
            mt5.shutdown()
            return False

        logger.info(
            f"[{self.label}] Connected — "
            f"Login: {info.login} | "
            f"Balance: {info.balance:.2f} {info.currency} | "
            f"Server: {self.server}"
        )
        self._connected = True
        return True

    def _reconnect_with_backoff(self) -> bool:
        wait = 1
        attempts = 0
        while True:
            attempts += 1
            logger.info(f"[{self.label}] Reconnect attempt {attempts} (wait={wait}s)…")
            # Tear down any half-open session before re-initialising the SAME
            # terminal/login — never another account.
            if MT5_AVAILABLE:
                try:
                    mt5.shutdown()
                except Exception:
                    pass
            if self._connect_inner():
                logger.success(f"[{self.label}] Reconnected successfully")
                return True
            time.sleep(wait)
            wait = min(wait * 2, self.max_wait_s)

    # ── MT5 helpers (pass-through with guard) ─────────────────────────

    def account_info(self):
        if not MT5_AVAILABLE:
            return _MockAccountInfo(self.login)
        return mt5.account_info()

    def positions_get(self):
        if not MT5_AVAILABLE:
            return []
        return mt5.positions_get() or []

    def orders_get(self):
        if not MT5_AVAILABLE:
            return []
        return mt5.orders_get() or []

    def order_send(self, request: dict):
        if not MT5_AVAILABLE:
            return _MockOrderResult(request)
        return mt5.order_send(request)

    def order_check(self, request: dict):
        if not MT5_AVAILABLE:
            return _MockOrderCheck(request)
        return mt5.order_check(request)

    def symbol_info(self, symbol: str):
        if not MT5_AVAILABLE:
            return _MockSymbolInfo(symbol)
        return mt5.symbol_info(symbol)

    def symbol_info_tick(self, symbol: str):
        if not MT5_AVAILABLE:
            return _MockTick(symbol)
        return mt5.symbol_info_tick(symbol)

    def symbol_select(self, symbol: str, enable: bool = True) -> bool:
        if not MT5_AVAILABLE:
            return True
        return mt5.symbol_select(symbol, enable)

    def last_error(self):
        if not MT5_AVAILABLE:
            return (0, "MOCK mode")
        return mt5.last_error()


# ── Mock objects for non-Windows / test environments ──────────────────

class _MockAccountInfo:
    def __init__(self, login):
        self.login = login
        self.balance = 10_000.0
        self.equity = 10_000.0
        self.margin_free = 10_000.0
        self.margin = 0.0
        self.currency = "USD"
        self.leverage = 100
        self.server = "MockServer"


class _MockSymbolInfo:
    def __init__(self, symbol):
        self.name = symbol
        self.point = 0.00001
        self.digits = 5
        self.trade_tick_size = 0.00001
        self.trade_tick_value = 1.0
        self.trade_contract_size = 100_000.0
        self.volume_min = 0.01
        self.volume_max = 100.0
        self.volume_step = 0.01
        self.trade_stops_level = 0
        self.trade_freeze_level = 0
        self.spread = 2
        self.visible = True
        self.filling_mode = 1


class _MockTick:
    def __init__(self, symbol):
        self.bid = 1.10000
        self.ask = 1.10002
        self.time = 0


class _MockOrderResult:
    def __init__(self, request: dict):
        # Deterministic mock ticket derived from the request — avoids
        # Math.random-style nondeterminism and is stable across a test run.
        self.retcode = 10009  # TRADE_RETCODE_DONE
        base = int(request.get("magic", 0)) or int(request.get("position", 0)) or 500000
        self.order = base + 1
        self.deal = base + 2
        self.volume = request.get("volume", 0.0)
        self.price = request.get("price", 0.0)
        self.comment = "mock"

    @property
    def retcode_description(self):
        return "TRADE_RETCODE_DONE (mock)"


class _MockOrderCheck:
    def __init__(self, request: dict):
        self.retcode = 0  # OK
        self.margin = 0.0
        self.margin_free = 10_000.0
        self.comment = "mock-ok"
