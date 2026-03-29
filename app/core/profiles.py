"""hb environments — Profile-based multi-environment configuration.

Like dbt profiles.yml: one dictionary, N environments.
Each environment specifies connection, engine, security profile.

PBI #549 — Feature #541.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class Environment(BaseModel):
    """A single environment configuration."""
    connection: str
    engine: str = "pg"
    profile: str = "essential"
    schema_filter: str | None = None
    description: str | None = None

    model_config = {"extra": "ignore"}

    @field_validator("engine")
    @classmethod
    def validate_engine(cls, v: str) -> str:
        allowed = {"pg", "mssql", "oracle", "redis"}
        if v not in allowed:
            raise ValueError(f"Engine '{v}' not supported. Available: {', '.join(sorted(allowed))}")
        return v

    @field_validator("profile")
    @classmethod
    def validate_profile(cls, v: str) -> str:
        allowed = {"essential", "standard", "enterprise"}
        if v not in allowed:
            raise ValueError(f"Profile '{v}' not supported. Available: {', '.join(sorted(allowed))}")
        return v


class ProfileConfig(BaseModel):
    """Top-level hb-profiles.yml model."""
    default_env: str = "dev"
    environments: dict[str, Environment]

    model_config = {"extra": "ignore"}

    @field_validator("environments")
    @classmethod
    def at_least_one_env(cls, v: dict[str, Environment]) -> dict[str, Environment]:
        if not v:
            raise ValueError("At least one environment must be defined")
        return v


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

PROFILES_FILENAME = "hb-profiles.yml"


def find_profiles(start_dir: str | Path | None = None) -> Path | None:
    """Search for hb-profiles.yml starting from start_dir, walking up to root."""
    current = Path(start_dir) if start_dir else Path.cwd()
    current = current.resolve()

    for parent in [current, *current.parents]:
        candidate = parent / PROFILES_FILENAME
        if candidate.is_file():
            return candidate

    return None


def load_profiles(path: str | Path) -> ProfileConfig:
    """Load and validate hb-profiles.yml."""
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError(f"Invalid profiles file: expected YAML mapping, got {type(raw).__name__}")

    return ProfileConfig.model_validate(raw)


def resolve_env(
    env_name: str | None = None,
    profiles_path: str | Path | None = None,
) -> Environment | None:
    """Resolve an environment by name.

    If env_name is None, returns None (caller should use explicit flags).
    If env_name is provided but profiles file not found, raises FileNotFoundError.
    """
    if env_name is None:
        return None

    if profiles_path:
        path = Path(profiles_path)
    else:
        path = find_profiles()

    if path is None:
        raise FileNotFoundError(
            f"No {PROFILES_FILENAME} found. Create one or use explicit flags "
            f"(--connection, --engine, --profile)."
        )

    config = load_profiles(path)

    if env_name not in config.environments:
        available = ", ".join(sorted(config.environments.keys()))
        raise ValueError(
            f"Environment '{env_name}' not found in {path}. "
            f"Available: {available}"
        )

    return config.environments[env_name]
