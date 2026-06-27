"""
tests/test_core.py
Unit tests for StateManager, Database, and lot calculation logic.
Run with: python -m pytest tests/ -v
"""

import pytest
import os
import tempfile
from datetime import datetime, timezone

from models.schemas import (
    Position, PendingOrder, TradeType, EventType,
    AccountConfig, LotMode, CopyMode,
)
from core.state_manager import StateManager
from database.db import Database


# ── Helpers ────────────────────────────────────────────────────────────

def make_position(ticket=1, symbol="EURUSD", volume=0.1, sl=0.0, tp=0.0,
                  trade_type=TradeType.BUY) -> Position:
    return Position(
        ticket=ticket,
        symbol=symbol,
        trade_type=trade_type,
        volume=volume,
        open_price=1.10000,
        sl=sl,
        tp=tp,
        profit=0.0,
        open_time=datetime.now(timezone.utc).replace(tzinfo=None),
    )


def make_pending(ticket=100, symbol="EURUSD", volume=0.1,
                 price=1.09000, sl=0.0, tp=0.0,
                 trade_type=TradeType.BUY_LIMIT) -> PendingOrder:
    return PendingOrder(
        ticket=ticket,
        symbol=symbol,
        trade_type=trade_type,
        volume=volume,
        price=price,
        sl=sl,
        tp=tp,
    )


# ── StateManager tests ──────────────────────────────────────────────────

class TestStateManager:

    def setup_method(self):
        self.sm = StateManager()

    def _init(self, positions=None, orders=None):
        """First call seeds state — returns empty list."""
        return self.sm.compare(positions or [], orders or [])

    def test_first_call_returns_no_events(self):
        pos = [make_position(1), make_position(2)]
        events = self._init(pos)
        assert events == []

    def test_new_position_detected(self):
        self._init([make_position(1)])
        events = self.sm.compare([make_position(1), make_position(2)], [])
        types = [e.event_type for e in events]
        assert EventType.NEW_POSITION in types
        assert any(e.master_ticket == 2 for e in events)

    def test_close_position_detected(self):
        self._init([make_position(1), make_position(2)])
        events = self.sm.compare([make_position(1)], [])
        types = [e.event_type for e in events]
        assert EventType.CLOSE_POSITION in types
        assert any(e.master_ticket == 2 for e in events)

    def test_modify_sl_tp_detected(self):
        self._init([make_position(1, sl=0.0, tp=0.0)])
        events = self.sm.compare([make_position(1, sl=1.09500, tp=1.11000)], [])
        assert len(events) == 1
        assert events[0].event_type == EventType.MODIFY_POSITION
        assert events[0].sl == pytest.approx(1.09500)

    def test_partial_close_detected(self):
        self._init([make_position(1, volume=0.2)])
        events = self.sm.compare([make_position(1, volume=0.1)], [])
        assert len(events) == 1
        assert events[0].event_type == EventType.PARTIAL_CLOSE
        assert events[0].close_volume == pytest.approx(0.1)

    def test_no_event_when_nothing_changes(self):
        self._init([make_position(1)])
        events = self.sm.compare([make_position(1)], [])
        assert events == []

    def test_new_pending_detected(self):
        self._init()
        events = self.sm.compare([], [make_pending(100)])
        assert len(events) == 1
        assert events[0].event_type == EventType.NEW_PENDING

    def test_delete_pending_detected(self):
        self._init([], [make_pending(100)])
        events = self.sm.compare([], [])
        assert len(events) == 1
        assert events[0].event_type == EventType.DELETE_PENDING

    def test_modify_pending_detected(self):
        self._init([], [make_pending(100, price=1.09000)])
        events = self.sm.compare([], [make_pending(100, price=1.08500)])
        assert len(events) == 1
        assert events[0].event_type == EventType.MODIFY_PENDING

    def test_reset_clears_state(self):
        self._init([make_position(1)])
        self.sm.reset()
        # After reset, first compare again seeds — no events
        events = self.sm.compare([make_position(1), make_position(2)], [])
        assert events == []

    def test_multiple_events_at_once(self):
        self._init([make_position(1), make_position(2)])
        # Close 1, modify 2, open 3
        events = self.sm.compare([
            make_position(2, sl=1.09500),
            make_position(3),
        ], [])
        event_types = {e.event_type for e in events}
        assert EventType.CLOSE_POSITION in event_types
        assert EventType.MODIFY_POSITION in event_types
        assert EventType.NEW_POSITION in event_types


# ── Database tests ──────────────────────────────────────────────────────

class TestDatabase:

    def setup_method(self):
        self.tmp = tempfile.mktemp(suffix=".db")
        self.db = Database(self.tmp)

    def teardown_method(self):
        self.db.close()
        if os.path.exists(self.tmp):
            os.remove(self.tmp)

    def test_ticket_mapping_round_trip(self):
        self.db.save_ticket_mapping(1001, 9999, 5555, "EURUSD")
        ticket = self.db.get_slave_ticket(1001, 9999)
        assert ticket == 5555

    def test_get_slave_ticket_missing(self):
        ticket = self.db.get_slave_ticket(9999, 8888)
        assert ticket is None

    def test_mark_mapping_closed(self):
        self.db.save_ticket_mapping(1001, 9999, 5555, "EURUSD")
        self.db.mark_mapping_closed(1001, 9999)
        ticket = self.db.get_slave_ticket(1001, 9999)
        assert ticket is None  # closed mapping not returned

    def test_event_dedup(self):
        self.db.enqueue_event("evt-001", "NEW_POSITION", "{}")
        assert self.db.event_exists("evt-001") is True
        assert self.db.event_exists("evt-999") is False

    def test_log_execution(self):
        self.db.log_execution(
            event_id="evt-001",
            slave_login=9999,
            master_ticket=1001,
            symbol="EURUSD",
            action="NEW_POSITION",
            status="SUCCESS",
            slave_ticket=5555,
            latency_ms=42.5,
        )
        rows = self.db.get_recent_executions(limit=5)
        assert len(rows) == 1
        assert rows[0]["status"] == "SUCCESS"
        assert rows[0]["latency_ms"] == pytest.approx(42.5)

    def test_stats(self):
        self.db.save_ticket_mapping(1001, 9999, 5555, "EURUSD")
        self.db.save_ticket_mapping(1002, 9999, 5556, "GBPUSD")
        self.db.mark_mapping_closed(1001, 9999)
        stats = self.db.get_stats()
        assert stats["open_trades"] == 1
        assert stats["closed_trades"] == 1

    def test_upsert_account(self):
        self.db.upsert_account(12345, "BrokerServer", "master", 10000.0)
        # Should not raise on second call
        self.db.upsert_account(12345, "BrokerServer", "master", 11000.0)

    def test_all_slave_tickets(self):
        self.db.save_ticket_mapping(1001, 1111, 5555, "EURUSD")
        self.db.save_ticket_mapping(1001, 2222, 6666, "EURUSD")
        rows = self.db.get_all_slave_tickets(1001)
        logins = {r["slave_login"] for r in rows}
        assert logins == {1111, 2222}


# ── Lot sizing tests ────────────────────────────────────────────────────

class TestLotCalculation:
    """Test lot calculation by simulating the Executor._calc_lot logic inline."""

    def _calc(self, mode, lot_value, master_lot,
               master_balance=10000.0, slave_balance=5000.0,
               min_lot=0.01, max_lot=100.0) -> float:
        """Inline copy of the lot calc logic for unit testing."""
        if mode == LotMode.FIXED:
            lot = lot_value
        elif mode == LotMode.MULTIPLIER:
            lot = master_lot * lot_value
        elif mode == LotMode.RATIO:
            ratio = slave_balance / master_balance if master_balance > 0 else 1.0
            lot = master_lot * ratio * lot_value
        elif mode == LotMode.RISK_PERCENT:
            lot = (slave_balance * lot_value / 100) / 1000
        else:
            lot = master_lot
        return round(max(min_lot, min(max_lot, lot)), 2)

    def test_fixed_lot(self):
        assert self._calc(LotMode.FIXED, 0.5, 1.0) == 0.5

    def test_multiplier(self):
        assert self._calc(LotMode.MULTIPLIER, 2.0, 0.1) == pytest.approx(0.2)

    def test_ratio_half_balance(self):
        # Slave has half the balance → gets half the lot
        result = self._calc(LotMode.RATIO, 1.0, 0.2, master_balance=10000, slave_balance=5000)
        assert result == pytest.approx(0.1)

    def test_lot_clamped_to_max(self):
        result = self._calc(LotMode.FIXED, 200.0, 1.0, max_lot=50.0)
        assert result == 50.0

    def test_lot_clamped_to_min(self):
        result = self._calc(LotMode.MULTIPLIER, 0.001, 0.01, min_lot=0.01)
        assert result == 0.01


# ── Mock-mode fixtures (force MT5 unavailable) ──────────────────────────

import uuid
import brokers.mt5_connection as mt5conn
from brokers.mt5_connection import MT5Connection
from core.executor import Executor
from models.schemas import TradeEvent


@pytest.fixture
def mock_mt5(monkeypatch):
    """Force the connection layer into MOCK mode regardless of MT5 install."""
    monkeypatch.setattr(mt5conn, "MT5_AVAILABLE", False)
    yield


@pytest.fixture
def db_tmp():
    import tempfile
    path = tempfile.mktemp(suffix=".db")
    db = Database(path)
    yield db
    db.close()
    if os.path.exists(path):
        os.remove(path)


def _new_event(ticket=12345, symbol="EURUSD", etype=EventType.NEW_POSITION,
               tt=TradeType.BUY, vol=0.1, price=1.10000, sl=1.09000, tp=1.12000):
    return TradeEvent(
        event_id=str(uuid.uuid4()), event_type=etype, master_ticket=ticket,
        symbol=symbol, trade_type=tt, volume=vol, price=price, sl=sl, tp=tp,
        master_balance=10000.0, master_equity=10000.0,
    )


# ── Durable per-slave queue ─────────────────────────────────────────────

class TestDurableQueue:

    def test_materialise_and_claim(self, db_tmp):
        ev = _new_event()
        db_tmp.enqueue_event(ev.event_id, ev.event_type.value, ev.model_dump_json())
        assert db_tmp.materialise_slave_events(9999) == 1
        # cursor advanced — no double materialise
        assert db_tmp.materialise_slave_events(9999) == 0
        due = db_tmp.get_due_slave_events(9999)
        assert len(due) == 1

    def test_claim_is_exclusive(self, db_tmp):
        ev = _new_event()
        db_tmp.enqueue_event(ev.event_id, ev.event_type.value, ev.model_dump_json())
        db_tmp.materialise_slave_events(9999)
        assert db_tmp.claim_slave_event(ev.event_id, 9999) is True
        # second claim loses
        assert db_tmp.claim_slave_event(ev.event_id, 9999) is False

    def test_retry_then_fail(self, db_tmp):
        ev = _new_event()
        db_tmp.enqueue_event(ev.event_id, ev.event_type.value, ev.model_dump_json())
        db_tmp.materialise_slave_events(9999)
        for _ in range(3):
            db_tmp.claim_slave_event(ev.event_id, 9999)
            db_tmp.retry_slave_event(ev.event_id, 9999, delay_ms=0, max_retries=3,
                                     error="boom")
        depth = db_tmp.get_queue_depth()
        assert depth.get("FAILED") == 1

    def test_recover_stuck(self, db_tmp):
        ev = _new_event()
        db_tmp.enqueue_event(ev.event_id, ev.event_type.value, ev.model_dump_json())
        db_tmp.materialise_slave_events(9999)
        db_tmp.claim_slave_event(ev.event_id, 9999)  # now PROCESSING
        n = db_tmp.recover_stuck_slave_events(9999, stale_seconds=0)
        assert n == 1
        assert len(db_tmp.get_due_slave_events(9999)) == 1


# ── Executor in mock mode ───────────────────────────────────────────────

class TestExecutor:

    def _make(self, db, mode=CopyMode.COPY, lot_mode=LotMode.FIXED, lot_value=0.5):
        conn = MT5Connection(login=9999, password="x", server="Mock", label="S")
        conn.connect()
        cfg = AccountConfig(login=9999, password="x", server="Mock", mode=mode,
                            lot_mode=lot_mode, lot_value=lot_value)
        return Executor(conn=conn, config=cfg, db=db, symbol_map={})

    def test_open_creates_mapping(self, mock_mt5, db_tmp):
        ex = self._make(db_tmp)
        res = ex.execute(_new_event())
        assert res.ok
        assert db_tmp.get_slave_ticket(12345, 9999) is not None

    def test_close_marks_mapping_closed(self, mock_mt5, db_tmp):
        ex = self._make(db_tmp)
        # No slave position exists in mock → close treated as already-gone success
        db_tmp.save_ticket_mapping(12345, 9999, 555, "EURUSD")
        res = ex.execute(_new_event(etype=EventType.CLOSE_POSITION))
        assert res.ok
        assert db_tmp.get_slave_ticket(12345, 9999) is None

    def test_reverse_mode_flips_direction(self, mock_mt5, db_tmp):
        ex = self._make(db_tmp, mode=CopyMode.REVERSE)
        assert ex._get_trade_type(TradeType.BUY) == TradeType.SELL
        assert ex._get_trade_type(TradeType.BUY_LIMIT) == TradeType.SELL_LIMIT

    def test_blacklist_skips(self, mock_mt5, db_tmp):
        ex = self._make(db_tmp)
        ex.risk.blacklist_symbols = ["EURUSD"]
        res = ex.execute(_new_event())
        assert res.ok and res.skipped

    def test_allowed_symbols_skips_others(self, mock_mt5, db_tmp):
        ex = self._make(db_tmp)
        ex.risk.allowed_symbols = ["GBPUSD"]
        res = ex.execute(_new_event(symbol="EURUSD"))
        assert res.skipped

    def test_lot_snapped_to_step_and_caps(self, mock_mt5, db_tmp):
        ex = self._make(db_tmp, lot_mode=LotMode.FIXED, lot_value=0.137)
        # mock volume_step is 0.01 → snaps to 0.14
        lot = ex._normalize_lot(0.137, "EURUSD")
        assert lot == pytest.approx(0.14)

    def test_risk_based_lot(self, mock_mt5, db_tmp):
        ex = self._make(db_tmp, lot_mode=LotMode.RISK_PERCENT, lot_value=1.0)
        ev = _new_event(price=1.10000, sl=1.09000)  # 100 pip SL (0.01000)
        # balance 10000 * 1% = 100 risk; loss_per_lot = (0.01/1e-5)*1.0 = 1000
        # lot = 100/1000 = 0.1
        lot = ex._risk_based_lot(10000.0, 1.0, ev, "EURUSD", master_lot=0.1)
        assert lot == pytest.approx(0.1, abs=0.02)


# ── Config loader ───────────────────────────────────────────────────────

class TestConfigLoader:

    def test_master_role(self, tmp_path, monkeypatch):
        from config.loader import load_config
        from models.schemas import Role
        p = tmp_path / "m.yaml"
        p.write_text(
            "role: master\nmaster:\n  login: 111\n  password: ${PW}\n  server: S\n"
            "settings:\n  db_path: x.db\n"
        )
        monkeypatch.setenv("PW", "secret")
        cfg = load_config(str(p))
        assert cfg.role == Role.MASTER
        assert cfg.master.login == 111
        assert cfg.master.password == "secret"  # env expanded

    def test_slave_role_with_risk(self, tmp_path):
        from config.loader import load_config
        from models.schemas import Role
        p = tmp_path / "s.yaml"
        p.write_text(
            "role: slave\nslaves:\n  - login: 222\n    password: p\n    server: S\n"
            "    terminal_path: C:/x/t.exe\n    risk:\n      max_spread_points: 30\n"
        )
        cfg = load_config(str(p))
        assert cfg.role == Role.SLAVE
        assert cfg.slaves[0].terminal_path == "C:/x/t.exe"
        assert cfg.slaves[0].risk.max_spread_points == 30

    def test_missing_role_raises(self, tmp_path):
        from config.loader import load_config
        p = tmp_path / "bad.yaml"
        p.write_text("master:\n  login: 1\n  password: p\n  server: S\n")
        with pytest.raises(ValueError):
            load_config(str(p))
