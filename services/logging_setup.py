"""
services/logging_setup.py
Configures loguru with per-role log files plus shared error and event logs.

    logs/master.log   — everything from the master process
    logs/slave.log    — everything from the slave process(es)
    logs/errors.log   — ERROR+ from any process
    logs/events.log   — trade lifecycle messages (opens/closes/modifies/queue)
"""

import sys
from pathlib import Path
from loguru import logger

_EVENT_KEYWORDS = (
    "Opened", "Closed", "Modified", "Partial", "Pending",
    "Event queued", "copy:", "close:", "Reconcile",
)


def setup_logging(log_level: str = "INFO", role: str = "app"):
    Path("logs").mkdir(exist_ok=True)

    logger.remove()  # remove default handler

    # Console
    logger.add(
        sys.stdout,
        level=log_level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{line}</cyan> — "
            "<level>{message}</level>"
        ),
        colorize=True,
    )

    # Per-role process log (master.log / slave.log; falls back to app.log).
    role_file = {"master": "master.log", "slave": "slave.log"}.get(role, "app.log")
    logger.add(
        f"logs/{role_file}",
        level=log_level,
        rotation="10 MB",
        retention="30 days",
        compression="zip",
        enqueue=True,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{line} — {message}",
    )

    # Errors only (shared across processes).
    logger.add(
        "logs/errors.log",
        level="ERROR",
        rotation="5 MB",
        retention="60 days",
        compression="zip",
        enqueue=True,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{line} — {message}\n{exception}",
    )

    # Trade/queue events only (filter by keyword in message).
    logger.add(
        "logs/events.log",
        level="DEBUG",
        rotation="10 MB",
        retention="90 days",
        compression="zip",
        enqueue=True,
        filter=lambda r: any(kw in r["message"] for kw in _EVENT_KEYWORDS),
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}",
    )

    logger.info(f"Logging initialised | role={role} | level={log_level} | file=logs/{role_file}")
