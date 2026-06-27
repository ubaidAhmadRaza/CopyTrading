"""
config/loader.py
Loads and validates the YAML configuration file.

A config declares a `role` (master | slave). One process serves exactly one
role against exactly one MT5 terminal — this is what keeps a single Python
process bound to a single MT5 connection (the core architectural fix).

Secrets may be supplied via ${ENV_VAR} placeholders in the YAML so that
passwords need not be committed to disk.
"""

from __future__ import annotations
import os
import re
from pathlib import Path
import yaml
from loguru import logger

from models.schemas import AppConfig, MasterConfig, AccountConfig, RiskConfig, Role

_ENV_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _expand_env(value):
    """Recursively expand ${ENV_VAR} placeholders in strings."""
    if isinstance(value, str):
        def _sub(m):
            var = m.group(1)
            if var not in os.environ:
                logger.warning(f"Env var '{var}' referenced in config is not set")
            return os.environ.get(var, "")
        return _ENV_PATTERN.sub(_sub, value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


def load_config(path: str = "config/config.yaml") -> AppConfig:
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(cfg_path, "r") as f:
        raw = yaml.safe_load(f) or {}

    raw = _expand_env(raw)

    role_raw = raw.get("role")
    if role_raw is None:
        raise ValueError(
            f"Config '{path}' is missing required field 'role' (master | slave)"
        )
    role = Role(str(role_raw).lower())

    settings = raw.get("settings", {})
    sym_map = raw.get("symbol_mapping", {})

    master = None
    slaves: list[AccountConfig] = []

    if role == Role.MASTER:
        master_raw = raw.get("master")
        if not master_raw:
            raise ValueError(f"Role is 'master' but no 'master:' block found in {path}")
        master = MasterConfig(
            login=master_raw["login"],
            password=master_raw["password"],
            server=master_raw["server"],
            terminal_path=master_raw.get("terminal_path"),
        )
    elif role == Role.SLAVE:
        slaves_raw = raw.get("slaves") or []
        if not slaves_raw:
            raise ValueError(f"Role is 'slave' but no 'slaves:' list found in {path}")
        for s in slaves_raw:
            risk_raw = s.pop("risk", {}) if isinstance(s, dict) else {}
            acct = AccountConfig(**s)
            if risk_raw:
                acct.risk = RiskConfig(**risk_raw)
            # default max_lot on the risk guard mirrors the account cap if unset
            if "max_lot" not in risk_raw:
                acct.risk.max_lot = acct.max_lot
            slaves.append(acct)

    config = AppConfig(
        role=role,
        master=master,
        slaves=slaves,
        symbol_mapping=sym_map,
        **{k: v for k, v in settings.items() if k in AppConfig.model_fields},
    )

    if role == Role.MASTER:
        logger.info(
            f"Config loaded [MASTER]: login={master.login} | "
            f"poll={config.poll_interval_ms}ms | db={config.db_path}"
        )
    else:
        logger.info(
            f"Config loaded [SLAVE]: {len(slaves)} account(s) | "
            f"sync={config.sync_interval_ms}ms | db={config.db_path}"
        )
    return config
