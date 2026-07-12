"""
Loads src/config/sources.yml and src/config/env.yml and resolves ${catalog}
placeholders for the active environment — the one thing every ingestion
call needs, resolved in one place instead of re-derived per notebook.

Usage:
    from common.config_loader import get_source_config, get_env_config

    env = get_env_config("dev")                     # {'catalog': 'vstone_traffic_dev', ...}
    src = get_source_config("chunk1_csv", env="dev")  # path/format/technique with ${catalog} resolved
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import yaml

_CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
_SOURCES_FILE = _CONFIG_DIR / "sources.yml"
_ENV_FILE = _CONFIG_DIR / "env.yml"

_VALID_ENVS = {"dev", "test", "prod"}


def _load_yaml(path: Path) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def _resolve_env_name(env: str | None) -> str:
    """Falls back to the DATABRICKS_BUNDLE_TARGET env var (set automatically when a
    job runs via DABs), then to 'dev' for local/manual runs."""
    resolved = env or os.environ.get("DATABRICKS_BUNDLE_TARGET") or "dev"
    if resolved not in _VALID_ENVS:
        raise ValueError(f"Unknown environment '{resolved}', expected one of {_VALID_ENVS}")
    return resolved


def get_env_config(env: str | None = None) -> Dict[str, Any]:
    envs = _load_yaml(_ENV_FILE)
    resolved = _resolve_env_name(env)
    if resolved not in envs:
        raise KeyError(f"No env.yml entry for '{resolved}'")
    return envs[resolved]


def _substitute(value: Any, replacements: Dict[str, str]) -> Any:
    """Replaces every ${key} in value with replacements[key]. Every string field in
    env.yml's active block is a usable placeholder -- e.g. raw_schema: ... there
    makes ${raw_schema} substitutable in sources.yml, with no code change needed
    when a new per-environment field is added."""
    if isinstance(value, str):
        for placeholder, replacement in replacements.items():
            value = value.replace(f"${{{placeholder}}}", replacement)
        return value
    if isinstance(value, dict):
        return {k: _substitute(v, replacements) for k, v in value.items()}
    if isinstance(value, list):
        return [_substitute(v, replacements) for v in value]
    return value


def get_source_config(key: str, env: str | None = None) -> Dict[str, Any]:
    """Some entries (e.g. bronze_streets) don't define their own path/format --
    they set source_key: raw_streets to reuse another entry's path/format/
    description, only overriding technique/target_table. Resolve that
    indirection here so every caller gets a single flat, complete config."""
    sources = _load_yaml(_SOURCES_FILE)
    if key not in sources:
        raise KeyError(f"No sources.yml entry for '{key}'. Known keys: {sorted(sources)}")

    cfg = dict(sources[key])
    referenced_key = cfg.get("source_key")
    if referenced_key:
        if referenced_key not in sources:
            raise KeyError(f"'{key}' references unknown source_key '{referenced_key}'")
        cfg = {**sources[referenced_key], **cfg}

    env_cfg = get_env_config(env)
    replacements = {k: v for k, v in env_cfg.items() if isinstance(v, str)}
    return _substitute(cfg, replacements)


def get_all_source_keys() -> list[str]:
    return sorted(_load_yaml(_SOURCES_FILE).keys())
