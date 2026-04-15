"""Ollama (local) provider — Hale-Bopp DB LLM Router.

Self-hosted / on-premise models via Ollama HTTP API.
"""

from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass, field

from .types import BaseProviderConfig, ProviderMessage

DEFAULT_MODEL = "llama3"
DEFAULT_BASE_URL = "http://localhost:11434"


@dataclass
class OllamaConfig(BaseProviderConfig):
    base_url: str = DEFAULT_BASE_URL
    model: str = DEFAULT_MODEL


def call_ollama(config: OllamaConfig, messages: list[ProviderMessage]) -> str:
    url = f"{config.base_url.rstrip('/')}/api/chat"

    payload = json.dumps({
        "model": config.model or DEFAULT_MODEL,
        "messages": [m.to_dict() for m in messages],
        "stream": False,
        "options": {
            "temperature": config.temperature if config.temperature is not None else 0.2,
            "num_predict": config.max_tokens or 2048,
        }
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    content = data.get("message", {}).get("content")
    if not content:
        raise RuntimeError("Ollama returned empty response")

    return content
