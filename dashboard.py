"""
dashboard.py
Standalone, read-only monitoring dashboard. Connects to NOTHING except the
shared SQLite database, so it can run alongside (or independently of) the
master and slave processes.

    python dashboard.py --config config/master.yaml
    python dashboard.py --db trade_copier.db
"""

from __future__ import annotations
import argparse
import sys

from database.db import Database
from services.dashboard import Dashboard


def main() -> int:
    parser = argparse.ArgumentParser(description="MT5 Trade Copier — dashboard")
    parser.add_argument("--config", help="Config file to read db_path from")
    parser.add_argument("--db", default=None, help="Path to the SQLite db (overrides config)")
    parser.add_argument("--refresh", type=float, default=2.0)
    args = parser.parse_args()

    db_path = args.db
    if db_path is None and args.config:
        from config.loader import load_config
        db_path = load_config(args.config).db_path
    db_path = db_path or "trade_copier.db"

    db = Database(db_path)
    dash = Dashboard(db=db, refresh_s=args.refresh)
    try:
        dash.run_blocking()
    except KeyboardInterrupt:
        pass
    finally:
        dash.stop()
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
