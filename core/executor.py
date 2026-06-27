"""
core/executor.py
Execution Engine — opens, modifies, and closes trades on a single slave
account. Includes risk-based lot sizing, broker-limit & margin verification,
symbol mapping, risk filters, and reverse-trade support.

IMPORTANT: The executor NEVER communicates with the master MT5 terminal. All
master context (balance/equity/volume) arrives inside the TradeEvent, captured
by the monitor at detection time. This is what allows master and slave to run
as fully independent processes.
"""

from __future__ import annotations
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from loguru import logger

from models.schemas import (
    TradeEvent, EventType, TradeType, CopyMode, LotMode, AccountConfig
)
from brokers.mt5_connection import MT5Connection
from database.db import Database


import MetaTrader5 as mt5  # noqa: F401  (constants only; resolved below)


# MT5 numeric constants (stable across the API).
TRADE_RETCODE_DONE = 10009
ORDER_TYPE_BUY = 0
ORDER_TYPE_SELL = 1
ORDER_TYPE_BUY_LIMIT = 2
ORDER_TYPE_SELL_LIMIT = 3
ORDER_TYPE_BUY_STOP = 4
ORDER_TYPE_SELL_STOP = 5
TRADE_ACTION_DEAL = 1
TRADE_ACTION_PENDING = 5
TRADE_ACTION_SLTP = 6
TRADE_ACTION_REMOVE = 8
ORDER_TIME_GTC = 1
ORDER_FILLING_IOC = 1
ORDER_FILLING_FOK = 0

_TYPE_MAP = {
    TradeType.BUY:        ORDER_TYPE_BUY,
    TradeType.SELL:       ORDER_TYPE_SELL,
    TradeType.BUY_LIMIT:  ORDER_TYPE_BUY_LIMIT,
    TradeType.SELL_LIMIT: ORDER_TYPE_SELL_LIMIT,
    TradeType.BUY_STOP:   ORDER_TYPE_BUY_STOP,
    TradeType.SELL_STOP:  ORDER_TYPE_SELL_STOP,
}

_REVERSE_MAP = {
    TradeType.BUY:        TradeType.SELL,
    TradeType.SELL:       TradeType.BUY,
    TradeType.BUY_LIMIT:  TradeType.SELL_LIMIT,
    TradeType.SELL_LIMIT: TradeType.BUY_LIMIT,
    TradeType.BUY_STOP:   TradeType.SELL_STOP,
    TradeType.SELL_STOP:  TradeType.BUY_STOP,
}

_MARKET_TYPES = {TradeType.BUY, TradeType.SELL}


@dataclass
class ExecResult:
    """Outcome of executing one event — carries retcode/error for logging."""
    ok: bool
    retcode: Optional[int] = None
    error: Optional[str] = None
    skipped: bool = False


class Executor:
    """Executes a single TradeEvent on a single slave account."""

    def __init__(
        self,
        conn: MT5Connection,
        config: AccountConfig,
        db: Database,
        symbol_map: dict[str, str],
        max_retries: int = 3,
        retry_delay_ms: int = 500,
    ):
        self.conn = conn
        self.config = config
        self.db = db
        self.symbol_map = symbol_map
        self.max_retries = max_retries
        self.retry_delay_ms = retry_delay_ms
        self.risk = config.risk

    # ── Entry point ────────────────────────────────────────────────────

    def execute(self, event: TradeEvent) -> ExecResult:
        """Execute an event on the slave account once (no retry loop — the
        synchronizer owns retry/backoff via the durable queue)."""
        if not self.config.enabled:
            return ExecResult(ok=True, skipped=True, error="slave disabled")

        if not self.conn.ensure_connected():
            return ExecResult(ok=False, error="slave not connected")

        # Risk filters apply only to opening actions.
        if event.event_type in (EventType.NEW_POSITION, EventType.NEW_PENDING):
            allowed, reason = self._risk_precheck(event)
            if not allowed:
                logger.warning(f"[{self.config.login}] Risk filter blocked: {reason}")
                return ExecResult(ok=True, skipped=True, error=reason)

        t_start = time.monotonic()
        try:
            result = self._dispatch(event)
        except Exception as exc:
            logger.exception(f"[{self.config.login}] Exception: {exc}")
            result = ExecResult(ok=False, error=str(exc))

        latency_ms = (time.monotonic() - t_start) * 1000
        status = "SUCCESS" if result.ok else ("SKIPPED" if result.skipped else "FAILED")
        self.db.log_execution(
            event_id=event.event_id,
            slave_login=self.config.login,
            master_ticket=event.master_ticket,
            symbol=event.symbol,
            action=event.event_type.value,
            status=status,
            error=result.error,
            latency_ms=latency_ms,
            retcode=result.retcode,
        )
        return result

    # ── Dispatcher ─────────────────────────────────────────────────────

    def _dispatch(self, event: TradeEvent) -> ExecResult:
        et = event.event_type
        handler = {
            EventType.NEW_POSITION:   self._open_position,
            EventType.MODIFY_POSITION: self._modify_position,
            EventType.PARTIAL_CLOSE:  self._partial_close,
            EventType.CLOSE_POSITION: self._close_position,
            EventType.NEW_PENDING:    self._open_pending,
            EventType.MODIFY_PENDING: self._modify_pending,
            EventType.DELETE_PENDING: self._delete_pending,
        }.get(et)
        if handler is None:
            return ExecResult(ok=False, error=f"unknown event type {et}")
        return handler(event)

    # ── Risk filters (P7) ──────────────────────────────────────────────

    def _risk_precheck(self, event: TradeEvent) -> tuple[bool, str]:
        symbol = self._map_symbol(event.symbol)

        if self.risk.allowed_symbols and symbol not in self.risk.allowed_symbols:
            return False, f"{symbol} not in allowed_symbols"
        if symbol in self.risk.blacklist_symbols:
            return False, f"{symbol} blacklisted"

        if not self._in_trading_session():
            return False, "outside trading session"

        if self.risk.max_spread_points > 0:
            info = self.conn.symbol_info(symbol)
            tick = self.conn.symbol_info_tick(symbol)
            if info and tick and info.point > 0:
                spread_pts = (tick.ask - tick.bid) / info.point
                if spread_pts > self.risk.max_spread_points:
                    return False, f"spread {spread_pts:.0f}pt > max {self.risk.max_spread_points:.0f}pt"

        if self.risk.max_daily_loss > 0 and self._daily_loss_exceeded():
            return False, "daily loss limit reached"

        return True, ""

    def _in_trading_session(self) -> bool:
        sessions = self.risk.trading_sessions
        if not sessions:
            return True
        now = datetime.now(timezone.utc).time()
        for window in sessions:
            try:
                start_s, end_s = window.split("-")
                sh, sm = (int(x) for x in start_s.split(":"))
                eh, em = (int(x) for x in end_s.split(":"))
                start = sh * 60 + sm
                end = eh * 60 + em
                cur = now.hour * 60 + now.minute
                if start <= end:
                    if start <= cur <= end:
                        return True
                else:  # window wraps midnight
                    if cur >= start or cur <= end:
                        return True
            except Exception:
                logger.warning(f"Bad trading_session window '{window}' — ignoring")
        return False

    def _daily_loss_exceeded(self) -> bool:
        acct = self.conn.account_info()
        if not acct:
            return False
        # Compare equity drop vs balance as a simple intraday-loss proxy.
        loss = float(getattr(acct, "balance", 0)) - float(getattr(acct, "equity", 0))
        return loss >= self.risk.max_daily_loss

    # ── Symbol & lot helpers ───────────────────────────────────────────

    def _map_symbol(self, symbol: str) -> str:
        mapped = self.symbol_map.get(symbol, symbol)
        # Ensure the symbol is selected in Market Watch before trading it.
        self.conn.symbol_select(mapped, True)
        return mapped

    def _calc_lot(self, master_lot: float, event: TradeEvent, symbol: str) -> float:
        mode = self.config.lot_mode
        val = self.config.lot_value
        slave_info = self.conn.account_info()
        slave_balance = float(getattr(slave_info, "balance", 0.0)) if slave_info else 0.0

        if mode == LotMode.FIXED:
            lot = val
        elif mode == LotMode.MULTIPLIER:
            lot = master_lot * val
        elif mode == LotMode.RATIO:
            # Balance-relative — master balance comes from the event, NOT a
            # live master connection.
            if event.master_balance > 0 and slave_balance > 0:
                ratio = slave_balance / event.master_balance
                lot = master_lot * ratio * val
            else:
                lot = master_lot
        elif mode == LotMode.RISK_PERCENT:
            lot = self._risk_based_lot(slave_balance, val, event, symbol, master_lot)
        else:
            lot = master_lot

        return self._normalize_lot(lot, symbol)

    def _risk_based_lot(self, balance, risk_pct, event, symbol, master_lot) -> float:
        """
        Real risk sizing (TODO #12):
            risk_money = balance * risk_pct%
            loss_per_lot = (SL_distance / tick_size) * tick_value * contract_factor
            lot = risk_money / loss_per_lot
        Falls back to master_lot if SL or symbol metadata is unavailable.
        """
        info = self.conn.symbol_info(symbol)
        if info is None or not event.sl or not event.price:
            logger.debug(f"[{self.config.login}] risk_percent fallback (no SL/info)")
            return master_lot

        tick_size = getattr(info, "trade_tick_size", 0.0) or getattr(info, "point", 0.0)
        tick_value = getattr(info, "trade_tick_value", 0.0)
        if tick_size <= 0 or tick_value <= 0:
            return master_lot

        sl_distance = abs(float(event.price) - float(event.sl))
        if sl_distance <= 0:
            return master_lot

        loss_per_lot = (sl_distance / tick_size) * tick_value
        if loss_per_lot <= 0:
            return master_lot

        risk_money = balance * (risk_pct / 100.0)
        lot = risk_money / loss_per_lot
        return lot

    def _normalize_lot(self, lot: float, symbol: str) -> float:
        """Clamp to broker volume limits and snap to the volume step (TODO #13)."""
        info = self.conn.symbol_info(symbol)
        vmin = getattr(info, "volume_min", self.config.min_lot) if info else self.config.min_lot
        vmax = getattr(info, "volume_max", self.config.max_lot) if info else self.config.max_lot
        vstep = getattr(info, "volume_step", 0.01) if info else 0.01

        # Apply per-slave config + risk caps on top of broker limits.
        vmin = max(vmin, self.config.min_lot)
        vmax = min(vmax, self.config.max_lot, self.risk.max_lot)

        lot = max(vmin, min(vmax, lot))
        if vstep > 0:
            steps = round(lot / vstep)
            lot = steps * vstep
        # Round to the step's precision to avoid float dust.
        decimals = max(0, len(str(vstep).split(".")[-1])) if vstep < 1 else 0
        return round(lot, decimals or 2)

    def _get_trade_type(self, trade_type: TradeType) -> TradeType:
        if self.config.mode == CopyMode.REVERSE:
            return _REVERSE_MAP.get(trade_type, trade_type)
        return trade_type

    def _get_fill_mode(self, symbol: str) -> int:
        info = self.conn.symbol_info(symbol)
        if info is None:
            return ORDER_FILLING_IOC
        filling = getattr(info, "filling_mode", ORDER_FILLING_IOC)
        return filling if filling else ORDER_FILLING_IOC

    # ── Broker-limit & margin verification (TODO #13, #14) ──────────────

    def _verify_stops(self, symbol: str, price: float, sl: float, tp: float,
                      is_buy: bool) -> tuple[float, float]:
        """Clamp SL/TP outside the broker stop level; returns (sl, tp)."""
        info = self.conn.symbol_info(symbol)
        if info is None:
            return sl, tp
        point = getattr(info, "point", 0.0)
        stop_level = getattr(info, "trade_stops_level", 0) or 0
        if point <= 0 or stop_level <= 0:
            return sl, tp
        min_dist = stop_level * point
        # Only adjust if a stop is set and too close.
        if sl:
            if is_buy and (price - sl) < min_dist:
                sl = round(price - min_dist, getattr(info, "digits", 5))
            elif not is_buy and (sl - price) < min_dist:
                sl = round(price + min_dist, getattr(info, "digits", 5))
        if tp:
            if is_buy and (tp - price) < min_dist:
                tp = round(price + min_dist, getattr(info, "digits", 5))
            elif not is_buy and (price - tp) < min_dist:
                tp = round(price - min_dist, getattr(info, "digits", 5))
        return sl, tp

    def _check_margin(self, request: dict) -> tuple[bool, str]:
        if not self.risk.check_margin:
            return True, ""
        check = self.conn.order_check(request)
        if check is None:
            return True, ""  # can't verify — let order_send decide
        # retcode 0 = OK. Some builds return TRADE_RETCODE_DONE.
        if getattr(check, "retcode", 0) not in (0, TRADE_RETCODE_DONE):
            return False, f"order_check retcode={check.retcode} ({getattr(check, 'comment', '')})"
        margin = getattr(check, "margin", None)
        free = getattr(check, "margin_free", None)
        if margin is not None and free is not None and margin > free:
            return False, f"insufficient margin (need {margin:.2f}, free {free:.2f})"
        return True, ""

    # ── Open market position ───────────────────────────────────────────

    def _open_position(self, event: TradeEvent) -> ExecResult:
        if event.trade_type is None or event.volume is None:
            return ExecResult(ok=False, error="missing trade_type/volume")

        symbol = self._map_symbol(event.symbol)
        trade_type = self._get_trade_type(event.trade_type)
        lot = self._calc_lot(event.volume, event, symbol)
        if lot <= 0:
            return ExecResult(ok=False, error="computed lot <= 0")

        tick = self.conn.symbol_info_tick(symbol)
        if tick is None:
            return ExecResult(ok=False, error=f"no tick for {symbol}")

        is_buy = trade_type == TradeType.BUY
        price = tick.ask if is_buy else tick.bid
        sl, tp = self._verify_stops(symbol, price, event.sl or 0.0, event.tp or 0.0, is_buy)

        request = {
            "action":        TRADE_ACTION_DEAL,
            "symbol":        symbol,
            "volume":        lot,
            "type":          _TYPE_MAP[trade_type],
            "price":         price,
            "sl":            sl,
            "tp":            tp,
            "deviation":     self.risk.max_slippage_points,
            "magic":         event.master_ticket,
            "comment":       f"copy:{event.master_ticket}",
            "type_time":     ORDER_TIME_GTC,
            "type_filling":  self._get_fill_mode(symbol),
        }

        ok, reason = self._check_margin(request)
        if not ok:
            return ExecResult(ok=False, error=reason)

        result = self.conn.order_send(request)
        if result and result.retcode == TRADE_RETCODE_DONE:
            slave_ticket = result.order
            self.db.save_ticket_mapping(event.master_ticket, self.config.login, slave_ticket, symbol)
            self.db.record_trade(
                master_ticket=event.master_ticket, slave_login=self.config.login,
                symbol=symbol, action="OPEN", slave_ticket=slave_ticket,
                volume=lot, price=price, sl=sl, tp=tp, result="SUCCESS",
            )
            logger.success(
                f"[{self.config.login}] Opened {trade_type.value} {lot} {symbol} "
                f"@ {price:.5f} | ticket={slave_ticket}"
            )
            return ExecResult(ok=True, retcode=result.retcode)

        rc = result.retcode if result else None
        return ExecResult(ok=False, retcode=rc, error=f"open failed retcode={rc}")

    # ── Modify SL/TP ──────────────────────────────────────────────────

    def _modify_position(self, event: TradeEvent) -> ExecResult:
        slave_ticket = self.db.get_slave_ticket(event.master_ticket, self.config.login)
        if slave_ticket is None:
            return ExecResult(ok=False, error=f"no mapping for master={event.master_ticket}")

        symbol = self._map_symbol(event.symbol)
        request = {
            "action":   TRADE_ACTION_SLTP,
            "symbol":   symbol,
            "position": slave_ticket,
            "sl":       event.sl or 0.0,
            "tp":       event.tp or 0.0,
        }
        result = self.conn.order_send(request)
        if result and result.retcode == TRADE_RETCODE_DONE:
            logger.success(f"[{self.config.login}] Modified {slave_ticket}: SL={event.sl} TP={event.tp}")
            return ExecResult(ok=True, retcode=result.retcode)
        rc = result.retcode if result else None
        return ExecResult(ok=False, retcode=rc, error=f"modify failed retcode={rc}")

    # ── Full close ────────────────────────────────────────────────────

    def _close_position(self, event: TradeEvent) -> ExecResult:
        slave_ticket = self.db.get_slave_ticket(event.master_ticket, self.config.login)
        if slave_ticket is None:
            return ExecResult(ok=False, error=f"no mapping for master={event.master_ticket}")

        symbol = self._map_symbol(event.symbol)
        positions = self.conn.positions_get()
        pos = next((p for p in positions if p.ticket == slave_ticket), None)
        if pos is None:
            logger.warning(f"[{self.config.login}] Position {slave_ticket} already gone")
            self.db.mark_mapping_closed(event.master_ticket, self.config.login)
            return ExecResult(ok=True, error="already closed")

        tick = self.conn.symbol_info_tick(symbol)
        if tick is None:
            return ExecResult(ok=False, error=f"no tick for {symbol}")
        price = tick.bid if pos.type == ORDER_TYPE_BUY else tick.ask
        close_type = ORDER_TYPE_SELL if pos.type == ORDER_TYPE_BUY else ORDER_TYPE_BUY

        request = {
            "action":       TRADE_ACTION_DEAL,
            "symbol":       symbol,
            "volume":       pos.volume,
            "type":         close_type,
            "position":     slave_ticket,
            "price":        price,
            "deviation":    self.risk.max_slippage_points,
            "magic":        event.master_ticket,
            "comment":      f"close:{event.master_ticket}",
            "type_time":    ORDER_TIME_GTC,
            "type_filling": self._get_fill_mode(symbol),
        }
        result = self.conn.order_send(request)
        if result and result.retcode == TRADE_RETCODE_DONE:
            self.db.mark_mapping_closed(event.master_ticket, self.config.login)
            self.db.record_trade(
                master_ticket=event.master_ticket, slave_login=self.config.login,
                symbol=symbol, action="CLOSE", slave_ticket=slave_ticket, result="SUCCESS",
            )
            logger.success(f"[{self.config.login}] Closed {slave_ticket}")
            return ExecResult(ok=True, retcode=result.retcode)
        rc = result.retcode if result else None
        return ExecResult(ok=False, retcode=rc, error=f"close failed retcode={rc}")

    # ── Partial close ─────────────────────────────────────────────────

    def _partial_close(self, event: TradeEvent) -> ExecResult:
        slave_ticket = self.db.get_slave_ticket(event.master_ticket, self.config.login)
        if slave_ticket is None:
            return ExecResult(ok=False, error=f"no mapping for master={event.master_ticket}")

        symbol = self._map_symbol(event.symbol)
        positions = self.conn.positions_get()
        pos = next((p for p in positions if p.ticket == slave_ticket), None)
        if pos is None:
            return ExecResult(ok=False, error="slave position not found")

        # Scale the master's closed volume to the slave's lot, capped at what
        # the slave actually holds.
        close_vol = self._calc_lot(event.close_volume or 0, event, symbol)
        close_vol = min(close_vol, pos.volume)
        if close_vol <= 0:
            return ExecResult(ok=False, error="computed close volume <= 0")

        tick = self.conn.symbol_info_tick(symbol)
        if tick is None:
            return ExecResult(ok=False, error=f"no tick for {symbol}")
        price = tick.bid if pos.type == ORDER_TYPE_BUY else tick.ask
        close_type = ORDER_TYPE_SELL if pos.type == ORDER_TYPE_BUY else ORDER_TYPE_BUY

        request = {
            "action":       TRADE_ACTION_DEAL,
            "symbol":       symbol,
            "volume":       close_vol,
            "type":         close_type,
            "position":     slave_ticket,
            "price":        price,
            "deviation":    self.risk.max_slippage_points,
            "magic":        event.master_ticket,
            "comment":      f"partial:{event.master_ticket}",
            "type_time":    ORDER_TIME_GTC,
            "type_filling": self._get_fill_mode(symbol),
        }
        result = self.conn.order_send(request)
        if result and result.retcode == TRADE_RETCODE_DONE:
            logger.success(f"[{self.config.login}] Partial close {close_vol} of {slave_ticket}")
            return ExecResult(ok=True, retcode=result.retcode)
        rc = result.retcode if result else None
        return ExecResult(ok=False, retcode=rc, error=f"partial close failed retcode={rc}")

    # ── Pending orders ────────────────────────────────────────────────

    def _open_pending(self, event: TradeEvent) -> ExecResult:
        if event.trade_type is None or event.volume is None or event.price is None:
            return ExecResult(ok=False, error="missing pending fields")

        symbol = self._map_symbol(event.symbol)
        trade_type = self._get_trade_type(event.trade_type)
        lot = self._calc_lot(event.volume, event, symbol)
        if lot <= 0:
            return ExecResult(ok=False, error="computed lot <= 0")

        is_buy = trade_type in (TradeType.BUY_LIMIT, TradeType.BUY_STOP)
        sl, tp = self._verify_stops(symbol, event.price, event.sl or 0.0, event.tp or 0.0, is_buy)

        request = {
            "action":       TRADE_ACTION_PENDING,
            "symbol":       symbol,
            "volume":       lot,
            "type":         _TYPE_MAP[trade_type],
            "price":        event.price,
            "sl":           sl,
            "tp":           tp,
            "deviation":    self.risk.max_slippage_points,
            "magic":        event.master_ticket,
            "comment":      f"pending:{event.master_ticket}",
            "type_time":    ORDER_TIME_GTC,
            "type_filling": self._get_fill_mode(symbol),
        }
        ok, reason = self._check_margin(request)
        if not ok:
            return ExecResult(ok=False, error=reason)

        result = self.conn.order_send(request)
        if result and result.retcode == TRADE_RETCODE_DONE:
            slave_ticket = result.order
            self.db.save_ticket_mapping(event.master_ticket, self.config.login, slave_ticket, symbol)
            logger.success(f"[{self.config.login}] Pending {trade_type.value} placed: {slave_ticket}")
            return ExecResult(ok=True, retcode=result.retcode)
        rc = result.retcode if result else None
        return ExecResult(ok=False, retcode=rc, error=f"pending open failed retcode={rc}")

    def _modify_pending(self, event: TradeEvent) -> ExecResult:
        slave_ticket = self.db.get_slave_ticket(event.master_ticket, self.config.login)
        if slave_ticket is None:
            return ExecResult(ok=False, error=f"no mapping for master={event.master_ticket}")

        symbol = self._map_symbol(event.symbol)
        request = {
            "action":   TRADE_ACTION_PENDING,
            "order":    slave_ticket,
            "symbol":   symbol,
            "price":    event.price or 0.0,
            "sl":       event.sl or 0.0,
            "tp":       event.tp or 0.0,
            "type_time": ORDER_TIME_GTC,
        }
        result = self.conn.order_send(request)
        if result and result.retcode == TRADE_RETCODE_DONE:
            logger.success(f"[{self.config.login}] Pending modified: {slave_ticket}")
            return ExecResult(ok=True, retcode=result.retcode)
        rc = result.retcode if result else None
        return ExecResult(ok=False, retcode=rc, error=f"pending modify failed retcode={rc}")

    def _delete_pending(self, event: TradeEvent) -> ExecResult:
        slave_ticket = self.db.get_slave_ticket(event.master_ticket, self.config.login)
        if slave_ticket is None:
            return ExecResult(ok=False, error=f"no mapping for master={event.master_ticket}")

        request = {"action": TRADE_ACTION_REMOVE, "order": slave_ticket}
        result = self.conn.order_send(request)
        if result and result.retcode == TRADE_RETCODE_DONE:
            self.db.mark_mapping_closed(event.master_ticket, self.config.login)
            logger.success(f"[{self.config.login}] Pending deleted: {slave_ticket}")
            return ExecResult(ok=True, retcode=result.retcode)
        rc = result.retcode if result else None
        return ExecResult(ok=False, retcode=rc, error=f"pending delete failed retcode={rc}")
