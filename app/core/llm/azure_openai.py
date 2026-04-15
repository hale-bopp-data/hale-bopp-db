"""Azure OpenAI provider — Hale-Bopp DB LLM Router."""

from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass

from .types import BaseProviderConfig, ProviderMessage

DEFAULT_API_VERSION = "2024-02-15-preview"
DEFAULT_MAX_TOKENS = 2048
DEFAULT_TEMPERATURE = 0.2


@dataclass
class AzureOpenAIConfig(BaseProviderConfig):
    """
    Azure OpenAI Provider Config.
    base_url: https://<resource>.openai.azure.com/openai/deployments/<deployment-id>
    api_version: defaults to 2024-02-15-preview
    """
    api_version: str = DEFAULT_API_VERSION


def call_azure_openai(config: AzureOpenAIConfig, messages: list[ProviderMessage]) -> str:
    url = f"{config.base_url.rstrip('/')}/chat/completions?api-version={config.api_version}"

    payload = json.dumps({
        "max_tokens": config.max_tokens or DEFAULT_MAX_TOKENS,
        "temperature": config.temperature if config.temperature is not None else DEFAULT_TEMPERATURE,
        "messages": [m.to_dict() for m in messages],
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "api-key": config.api_key,
        },
        method="POST",
    )

    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    if "error" in data:
        raise RuntimeError(f"Azure OpenAI error: {data['error'].get('message', 'Unknown')}")

    content = data.get("choices", [{}])[0].get("message", {}).get("content")
    if not content:
        raise RuntimeError("Azure OpenAI returned empty response")

    return content
