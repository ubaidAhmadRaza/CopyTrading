"""
core/state_manager.py
Compares two snapshots of the master account state and emits TradeEvents
for every meaningful change detected.
"""

from __future__ import annotations
import uuid
from typing import Optional
from loguru import logger

from models.schemas import (
    Position, PendingOrder, TradeEvent, EventType, TradeType
)


def _trade_type_from_mt5(mt5_type: int) -> Optional[TradeType]:
    mapping = {
        0: TradeType.BUY,
        1: TradeType.SELL,
        2: TradeType.BUY_LIMIT,
        3: TradeType.SELL_LIMIT,
        4: TradeType.BUY_STOP,
        5: TradeType.SELL_STOP,
    }
    return mapping.get(mt5_type)


def parse_position(p) -> Position:
    """Convert an MT5 position object to a Position model."""
    return Position(
        ticket=p.ticket,
        symbol=p.symbol,
        trade_type=_trade_type_from_mt5(p.type) or TradeType.BUY,
        volume=round(p.volume, 2),
        open_price=p.price_open,
        sl=p.sl,
        tp=p.tp,
        profit=p.profit,
        open_time=p.time,          # already a datetime via MT5 lib
        comment=p.comment or "",
        magic=p.magic,
    )


def parse_order(o) -> PendingOrder:
    """Convert an MT5 order object to a PendingOrder model."""
    return PendingOrder(
        ticket=o.ticket,
        symbol=o.symbol,
        trade_type=_trade_type_from_mt5(o.type) or TradeType.BUY_LIMIT,
        volume=round(o.volume_current, 2),
        price=o.price_open,
        sl=o.sl,
        tp=o.tp,
        comment=o.comment or "",
        magic=o.magic,
    )


# ─────────────────────────────────────────────────────────────────────


class StateManager:
    """
    Holds the previous snapshot of the master account and produces
    a list of TradeEvent objects every time compare() is called
    with a new snapshot.
    """

    def __init__(self):
        self._positions: dict[int, Position] = {}        # ticket → Position
        self._orders: dict[int, PendingOrder] = {}       # ticket → PendingOrder
        self._initialised = False

    # ── Public ────────────────────────────────────────────────────────

    def compare(
        self,
        new_positions: list[Position],
        new_orders: list[PendingOrder],
    ) -> list[TradeEvent]:
        """
        Diff new_positions / new_orders against the stored state.
        Returns a (possibly empty) list of TradeEvents to process.
        """
        events: list[TradeEvent] = []
        new_pos_map = {p.ticket: p for p in new_positions}
        new_ord_map = {o.ticket: o for o in new_orders}

        if not self._initialised:
            # First call — seed state without emitting events
            self._positions = new_pos_map
            self._orders = new_ord_map
            self._initialised = True
            logger.info(
                f"State initialised: {len(new_pos_map)} positions, "
                f"{len(new_ord_map)} pending orders"
            )
            return []

        # ── Positions ──────────────────────────────────────────────
        events += self._diff_positions(new_pos_map)

        # ── Pending orders ─────────────────────────────────────────
        events += self._diff_orders(new_ord_map)

        # ── Update stored state ────────────────────────────────────
        self._positions = new_pos_map
        self._orders = new_ord_map

        return events

    def reset(self):
        """Called on reconnect to force a fresh baseline."""
        self._positions.clear()
        self._orders.clear()
        self._initialised = False
        logger.info("StateManager reset — next poll will re-baseline")

    # ── Positions diff ─────────────────────────────────────────────

    def _diff_positions(
        self, new_map: dict[int, Position]
    ) -> list[TradeEvent]:
        events: list[TradeEvent] = []
        old_map = self._positions

        for ticket, new_pos in new_map.items():
            if ticket not in old_map:
                # Brand-new position
                events.append(self._make_event(
                    EventType.NEW_POSITION,
                    new_pos.ticket,
                    new_pos.symbol,
                    trade_type=new_pos.trade_type,
                    volume=new_pos.volume,
                    price=new_pos.open_price,
                    sl=new_pos.sl,
                    tp=new_pos.tp,
                ))
            else:
                old_pos = old_map[ticket]
                # SL/TP or volume change
                sl_changed = abs(new_pos.sl - old_pos.sl) > 1e-8
                tp_changed = abs(new_pos.tp - old_pos.tp) > 1e-8
                vol_decreased = new_pos.volume < old_pos.volume - 1e-4

                if vol_decreased:
                    # Partial close
                    events.append(self._make_event(
                        EventType.PARTIAL_CLOSE,
                        ticket,
                        new_pos.symbol,
                        close_volume=round(old_pos.volume - new_pos.volume, 2),
                        volume=new_pos.volume,
                    ))
                elif sl_changed or tp_changed:
                    events.append(self._make_event(
                        EventType.MODIFY_POSITION,
                        ticket,
                        new_pos.symbol,
                        sl=new_pos.sl,
                        tp=new_pos.tp,
                    ))

        for ticket, old_pos in old_map.items():
            if ticket not in new_map:
                # Position fully closed
                events.append(self._make_event(
                    EventType.CLOSE_POSITION,
                    ticket,
                    old_pos.symbol,
                    volume=old_pos.volume,
                ))

        return events

    # ── Orders diff ────────────────────────────────────────────────

    def _diff_orders(
        self, new_map: dict[int, PendingOrder]
    ) -> list[TradeEvent]:
        events: list[TradeEvent] = []
        old_map = self._orders

        for ticket, new_ord in new_map.items():
            if ticket not in old_map:
                events.append(self._make_event(
                    EventType.NEW_PENDING,
                    ticket,
                    new_ord.symbol,
                    trade_type=new_ord.trade_type,
                    volume=new_ord.volume,
                    price=new_ord.price,
                    sl=new_ord.sl,
                    tp=new_ord.tp,
                ))
            else:
                old_ord = old_map[ticket]
                changed = (
                    abs(new_ord.price - old_ord.price) > 1e-8
                    or abs(new_ord.sl - old_ord.sl) > 1e-8
                    or abs(new_ord.tp - old_ord.tp) > 1e-8
                    or abs(new_ord.volume - old_ord.volume) > 1e-4
                )
                if changed:
                    events.append(self._make_event(
                        EventType.MODIFY_PENDING,
                        ticket,
                        new_ord.symbol,
                        price=new_ord.price,
                        sl=new_ord.sl,
                        tp=new_ord.tp,
                        volume=new_ord.volume,
                    ))

        for ticket, old_ord in old_map.items():
            if ticket not in new_map:
                events.append(self._make_event(
                    EventType.DELETE_PENDING,
                    ticket,
                    old_ord.symbol,
                ))

        return events

    # ── Factory helper ─────────────────────────────────────────────

    @staticmethod
    def _make_event(
        event_type: EventType,
        master_ticket: int,
        symbol: str,
        **kwargs,
    ) -> TradeEvent:
        event = TradeEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            master_ticket=master_ticket,
            symbol=symbol,
            **kwargs,
        )
        logger.debug(f"Event detected: {event_type.value} | ticket={master_ticket} | {symbol}")
        return event
