"""
Loads src/config/sources.yml and src/config/env.yml and resolves ${catalog}
placeholders for the active environment — the one thing every ingestion
call needs, resolved in one place instead of re-derived per notebook.

Usage:
    from utils.config_loader import get_source_config, get_env_config

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


def get_source_schema(source_key: str):
    """Returns the explicit, string-typed StructType for source_key (see
    config/schemas.py). Resolves the same source_key indirection as
    get_source_config() -- e.g. bronze_streets has no schema of its own, it
    reuses raw_streets' via the same source_key: raw_streets pointer."""
    from config.schemas import SCHEMAS

    sources = _load_yaml(_SOURCES_FILE)
    if source_key not in sources:
        raise KeyError(f"No sources.yml entry for '{source_key}'. Known keys: {sorted(sources)}")

    lookup_key = sources[source_key].get("source_key", source_key)
    if lookup_key not in SCHEMAS:
        raise KeyError(
            f"No schema defined for '{lookup_key}' (resolved from '{source_key}'). Known: {sorted(SCHEMAS)}"
        )
    return SCHEMAS[lookup_key]


def get_column_comments(source_key: str) -> Dict[str, str]:
    """Column-level descriptions for source_key (see config/column_comments.py).
    Resolves the same source_key indirection as get_source_schema() -- e.g.
    bronze_streets has no comments of its own, it reuses raw_streets' via the
    same source_key: raw_streets pointer. Returns {} (not a KeyError) for a
    source with no comments defined yet, since this is discoverability
    metadata, not something a pipeline run should fail over."""
    from config.column_comments import COLUMN_COMMENTS

    sources = _load_yaml(_SOURCES_FILE)
    if source_key not in sources:
        raise KeyError(f"No sources.yml entry for '{source_key}'. Known keys: {sorted(sources)}")

    lookup_key = sources[source_key].get("source_key", source_key)
    return COLUMN_COMMENTS.get(lookup_key, {})


def get_silver_column_comments(source_key: str) -> Dict[str, str]:
    """Column-level descriptions for a Silver table (see
    config/column_comments.py's SILVER_COLUMN_COMMENTS). Direct lookup, no
    indirection -- matches get_silver_schema()'s convention."""
    from config.column_comments import SILVER_COLUMN_COMMENTS

    return SILVER_COLUMN_COMMENTS.get(source_key, {})


def get_silver_schema(source_key: str):
    """Return the strict, typed StructType for a Silver table.

    Args:
        source_key: Silver sources.yml key, e.g. "silver_locations".

    Returns:
        StructType: the strict schema from config/silver_schemas.py.

    Notes:
        Deliberately a separate function from get_source_schema() (Bronze's
        permissive STRING schemas) rather than one function branching on
        layer -- keeps a caller from accidentally receiving a permissive
        schema where a strict one is required, or vice versa.
    """
    from config.silver_schemas import SILVER_SCHEMAS

    if source_key not in SILVER_SCHEMAS:
        raise KeyError(f"No Silver schema defined for '{source_key}'. Known: {sorted(SILVER_SCHEMAS)}")
    return SILVER_SCHEMAS[source_key]


def get_all_source_keys() -> list[str]:
    return sorted(_load_yaml(_SOURCES_FILE).keys())
