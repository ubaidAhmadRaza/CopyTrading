"""
slave_service.py
SLAVE process. Owns exactly ONE MT5 connection (the slave account) and runs the
synchronizer, which consumes events from the durable SQLite queue and executes
them locally. It NEVER connects to the master account.

Although the config schema allows a list of slaves, the one-connection-per-
process rule means each slave process drives exactly ONE account. If a config
lists multiple slaves, run one slave process per entry (each with its own
terminal_path). This service uses the first ENABLED slave in the given config.

Run directly:
    python slave_service.py --config config/slave.yaml
or via the role dispatcher:
    python app.py --config config/slave.yaml
"""

from __future__ import annotations
import sys
from loguru import logger

from models.schemas import AppConfig
from brokers.mt5_connection import MT5Connection
from core.executor import Executor
from core.synchronizer import Synchronizer
from database.db import Database
from services.reliability import ShutdownHandler, Watchdog


def _select_slave(cfg: AppConfig):
    """One process = one MT5 terminal, so pick a single enabled slave."""
    enabled = [s for s in cfg.slaves if s.enabled]
    if not enabled:
        return None
    if len(enabled) > 1:
        logger.warning(
            f"{len(enabled)} enabled slaves in config — a process binds ONE "
            f"terminal. Using login={enabled[0].login}; run separate processes "
            f"(and configs) for the others."
        )
    return enabled[0]


def run(cfg: AppConfig) -> int:
    logger.info("=" * 60)
    logger.info("  MT5 Trade Copier — SLAVE process")
    logger.info("=" * 60)

    slave_cfg = _select_slave(cfg)
    if slave_cfg is None:
        logger.critical("No enabled slave account in config — aborting")
        return 1

    db = Database(cfg.db_path)

    conn = MT5Connection(
        login=slave_cfg.login,
        password=slave_cfg.password,
        server=slave_cfg.server,
        terminal_path=slave_cfg.terminal_path,
        label=f"SLAVE-{slave_cfg.login}",
        max_wait_s=cfg.reconnect_max_wait_s,
    )
    if not conn.connect():
        logger.critical(f"Cannot connect to slave {slave_cfg.login} — aborting")
        return 1

    info = conn.account_info()
    if info:
        db.upsert_account(info.login, slave_cfg.server, "slave",
                          info.balance, getattr(info, "equity", info.balance))

    executor = Executor(
        conn=conn,
        config=slave_cfg,
        db=db,
        symbol_map=cfg.symbol_mapping,
        max_retries=cfg.max_retries,
        retry_delay_ms=cfg.retry_delay_ms,
    )

    synchronizer = Synchronizer(
        executors=[executor],
        db=db,
        sync_interval_ms=cfg.sync_interval_ms,
        reconcile_interval_s=cfg.reconcile_interval_s,
        max_retries=cfg.max_retries,
        retry_delay_ms=cfg.retry_delay_ms,
        heartbeat_interval_s=cfg.heartbeat_interval_s,
    )

    shutdown = ShutdownHandler()
    shutdown.install()

    watchdog = Watchdog(interval_s=cfg.heartbeat_interval_s)
    watchdog.register("synchronizer", synchronizer.is_alive, synchronizer.start)

    synchronizer.start()
    watchdog.start()

    def _cleanup():
        synchronizer.stop()
        watchdog.stop()
        db.heartbeat(f"slave:{slave_cfg.login}", "STOPPED")
        conn.disconnect()
        db.close()
        logger.info("Slave process stopped cleanly")

    shutdown.on_shutdown(_cleanup)

    logger.success(f"Slave {slave_cfg.login} running — press Ctrl+C to stop")
    shutdown.wait()
    return 0


if __name__ == "__main__":
    import argparse
    from config.loader import load_config
    from services.logging_setup import setup_logging

    parser = argparse.ArgumentParser(description="MT5 Trade Copier — slave")
    parser.add_argument("--config", default="config/slave.yaml")
    args = parser.parse_args()

    _cfg = load_config(args.config)
    setup_logging(_cfg.log_level, role="slave")
    sys.exit(run(_cfg))
