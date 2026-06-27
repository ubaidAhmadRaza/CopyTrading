"""
master_service.py
MASTER process. Owns exactly ONE MT5 connection (the master account) and runs
the monitor, which detects trade changes and writes them to the durable SQLite
queue. It NEVER executes trades and never touches slave accounts.

Run directly:
    python master_service.py --config config/master.yaml
or via the role dispatcher:
    python app.py --config config/master.yaml
"""

from __future__ import annotations
import sys
from loguru import logger

from models.schemas import AppConfig
from brokers.mt5_connection import MT5Connection
from core.state_manager import StateManager
from core.monitor import MT5Monitor
from database.db import Database
from services.reliability import ShutdownHandler, Watchdog


def run(cfg: AppConfig) -> int:
    logger.info("=" * 60)
    logger.info("  MT5 Trade Copier — MASTER process")
    logger.info("=" * 60)

    if cfg.master is None:
        logger.critical("No master config present — aborting")
        return 1

    db = Database(cfg.db_path)

    master_conn = MT5Connection(
        login=cfg.master.login,
        password=cfg.master.password,
        server=cfg.master.server,
        terminal_path=cfg.master.terminal_path,
        label="MASTER",
        max_wait_s=cfg.reconnect_max_wait_s,
    )
    if not master_conn.connect():
        logger.critical("Cannot connect to master MT5 — aborting")
        return 1

    info = master_conn.account_info()
    if info:
        db.upsert_account(info.login, cfg.master.server, "master",
                          info.balance, getattr(info, "equity", info.balance))

    state_manager = StateManager()
    monitor = MT5Monitor(
        master_conn=master_conn,
        state_manager=state_manager,
        db=db,
        poll_interval_ms=cfg.poll_interval_ms,
        heartbeat_interval_s=cfg.heartbeat_interval_s,
    )

    shutdown = ShutdownHandler()
    shutdown.install()

    watchdog = Watchdog(interval_s=cfg.heartbeat_interval_s)
    watchdog.register("monitor", monitor.is_alive, monitor.start)

    monitor.start()
    watchdog.start()

    def _cleanup():
        monitor.stop()
        watchdog.stop()
        db.heartbeat("master", "STOPPED")
        master_conn.disconnect()
        db.close()
        logger.info("Master process stopped cleanly")

    shutdown.on_shutdown(_cleanup)

    logger.success("Master running — press Ctrl+C to stop")
    shutdown.wait()
    return 0


if __name__ == "__main__":
    import argparse
    from config.loader import load_config
    from services.logging_setup import setup_logging

    parser = argparse.ArgumentParser(description="MT5 Trade Copier — master")
    parser.add_argument("--config", default="config/master.yaml")
    args = parser.parse_args()

    _cfg = load_config(args.config)
    setup_logging(_cfg.log_level, role="master")
    sys.exit(run(_cfg))
