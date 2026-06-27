"""
app.py
Role dispatcher for the MT5 Trade Copier.

One Python process drives exactly ONE MT5 terminal. The config's `role` field
decides whether this process runs as the master (monitor) or a slave
(synchronizer/executor):

    python app.py --config config/master.yaml    # master process
    python app.py --config config/slave.yaml     # slave process

Run the two side by side (one terminal each) to copy trades. This is the fix
for the single-connection-per-process limitation of the MetaTrader5 API.
"""

from __future__ import annotations
import argparse
import sys

from loguru import logger

from config.loader import load_config
from services.logging_setup import setup_logging
from models.schemas import Role


def main() -> int:
    parser = argparse.ArgumentParser(description="MT5 Trade Copier")
    parser.add_argument("--config", required=True,
                        help="Path to a role-specific config (master.yaml / slave.yaml)")
    args = parser.parse_args()

    cfg = load_config(args.config)
    setup_logging(cfg.log_level, role=cfg.role.value)

    if cfg.role == Role.MASTER:
        import master_service
        return master_service.run(cfg)
    elif cfg.role == Role.SLAVE:
        import slave_service
        return slave_service.run(cfg)
    else:
        logger.critical(f"Unknown role: {cfg.role}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
