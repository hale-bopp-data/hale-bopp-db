"""OpenRouter provider — Hale-Bopp DB LLM Router."""

from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass

from .types import BaseProviderConfig, ProviderMessage

DEFAULT_MODEL = "anthropic/claude-haiku-4-5"
DEFAULT_BASE_URL = "https://openrouter.ai/api/v1"
DEFAULT_MAX_TOKENS = 2048
DEFAULT_TEMPERATURE = 0.2


@dataclass
class OpenRouterConfig(BaseProviderConfig):
    pass


def call_openrouter(config: OpenRouterConfig, messages: list[ProviderMessage]) -> str:
    base_url = config.base_url or DEFAULT_BASE_URL
    url = f"{base_url.rstrip('/')}/chat/completions"

    payload = json.dumps({
        "model": config.model or DEFAULT_MODEL,
        "max_tokens": config.max_tokens or DEFAULT_MAX_TOKENS,
        "temperature": config.temperature if config.temperature is not None else DEFAULT_TEMPERATURE,
        "messages": [m.to_dict() for m in messages],
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {config.api_key}",
            "HTTP-Referer": "https://hale-bopp.dev",
            "X-Title": "Hale-Bopp DB Agentic Observer",
        },
        method="POST",
    )

    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    if "error" in data:
        raise RuntimeError(f"OpenRouter error: {data['error'].get('message', 'Unknown')}")

    content = data.get("choices", [{}])[0].get("message", {}).get("content")
    if not content:
        raise RuntimeError("OpenRouter returned empty response")

    return content
