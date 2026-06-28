"""
dashboard_service.py
Standalone dashboard process. Reads ONLY from SQLite, never connects to MT5.
Run this alongside master/slave processes:
    python dashboard_service.py --config config/master.yaml
"""

from __future__ import annotations
import argparse
import sys
import time
from loguru import logger

from config.loader import load_config
from services.logging_setup import setup_logging
from services.dashboard import Dashboard
from database.db import Database
from services.reliability import ShutdownHandler


def run(cfg) -> int:
    logger.info("=" * 60)
    logger.info("  MT5 Trade Copier — DASHBOARD")
    logger.info("=" * 60)

    db = Database(cfg.db_path)
    dashboard = Dashboard(db, refresh_s=2.0)

    shutdown = ShutdownHandler()
    shutdown.install()

    def _cleanup():
        dashboard.stop()
        db.close()
        logger.info("Dashboard stopped cleanly")

    shutdown.on_shutdown(_cleanup)

    # Run dashboard in blocking mode (it takes over the terminal)
    logger.success("Dashboard running — press Ctrl+C to stop")
    dashboard.run_blocking()

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MT5 Trade Copier - Dashboard")
    parser.add_argument("--config", required=True,
                        help="Path to config file (master.yaml or slave.yaml)")
    args = parser.parse_args()

    cfg = load_config(args.config)
    setup_logging(cfg.log_level, role="dashboard")
    sys.exit(run(cfg))