"""
LLM Provider types — Hale-Bopp DB Agentic Observer.

Mirrors the Valentino Engine provider pattern (BaseProviderConfig)
but in Python for the FastAPI backend.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ProviderMessage:
    role: str  # 'system' | 'user' | 'assistant'
    content: str

    def to_dict(self) -> dict[str, str]:
        return {"role": self.role, "content": self.content}


@dataclass
class BaseProviderConfig:
    api_key: str = ""
    model: str = ""
    base_url: str = ""
    max_tokens: int = 2048
    temperature: float = 0.2
