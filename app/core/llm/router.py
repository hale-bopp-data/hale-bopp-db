"""
LLM Router — Hale-Bopp DB Agentic Observer (PBI-2).

Mirrors the Valentino Engine provider pattern.
Reads provider config from environment variables with zero hardcoding.

Provider selection via HBDB_LLM_PROVIDER env var:
  - "openrouter"   → OpenRouter (default)
  - "azure"        → Azure OpenAI
  - "ollama"       → Ollama (on-premise / bare-metal)

Environment variables:
  HBDB_LLM_PROVIDER     = openrouter | azure | ollama
  HBDB_LLM_API_KEY      = <api key>
  HBDB_LLM_MODEL        = <model name>
  HBDB_LLM_BASE_URL     = <override base url>
  HBDB_LLM_MAX_TOKENS   = <integer>
  HBDB_LLM_TEMPERATURE  = <float>
  HBDB_AZURE_API_VERSION = <azure api version>
"""

from __future__ import annotations

import json
import os
from typing import Any

from .azure_openai import AzureOpenAIConfig, call_azure_openai
from .ollama import OllamaConfig, call_ollama
from .openrouter import OpenRouterConfig, call_openrouter
from .types import ProviderMessage

SCHEMA_OBSERVER_SYSTEM_PROMPT = """
You are the Hale-Bopp DB Agentic Schema Observer — an expert database architect assistant.

You have access to the complete Data Dictionary JSON of the database schema.
Answer questions about the schema accurately and concisely.

Rules (Testudo Formation):
- You MUST NOT generate or suggest raw SQL DDL statements (ALTER TABLE, DROP, etc.)
- You MUST NOT connect to the database directly
- You CAN analyze relationships, data types, PII fields, orphan tables, impact analysis
- You CAN suggest changes as modifications to the JSON dictionary fields only
- Your output must be clear, structured Markdown
- If you suggest a change, emit a fenced ```json block that the UI can apply safely

When asked to suggest schema changes, format them as:
```json
{
  "entity": "<table_name>",
  "change": "add_column | drop_column | rename_column | add_table | drop_table",
  "field": { "name": "<column_name>", "type": "<logical_type>" },
  "rename_to": "<new_column_name_if_needed>"
}
```

Never output SQL. Only propose dictionary mutations in JSON.
""".strip()


def _get_provider_config() -> tuple[str, Any]:
    provider = os.getenv("HBDB_LLM_PROVIDER", "openrouter").lower()
    api_key = os.getenv("HBDB_LLM_API_KEY", "")
    model = os.getenv("HBDB_LLM_MODEL", "")
    base_url = os.getenv("HBDB_LLM_BASE_URL", "")
    max_tokens = int(os.getenv("HBDB_LLM_MAX_TOKENS", "2048"))
    temperature = float(os.getenv("HBDB_LLM_TEMPERATURE", "0.2"))

    if provider == "azure":
        api_version = os.getenv("HBDB_AZURE_API_VERSION", "2024-02-15-preview")
        return "azure", AzureOpenAIConfig(
            api_key=api_key,
            model=model,
            base_url=base_url,
            max_tokens=max_tokens,
            temperature=temperature,
            api_version=api_version,
        )
    elif provider == "ollama":
        return "ollama", OllamaConfig(
            model=model or "llama3",
            base_url=base_url or "http://localhost:11434",
            max_tokens=max_tokens,
            temperature=temperature,
        )
    else:
        # Default: openrouter
        return "openrouter", OpenRouterConfig(
            api_key=api_key,
            model=model,
            base_url=base_url,
            max_tokens=max_tokens,
            temperature=temperature,
        )


def _call_provider(provider: str, config: Any, messages: list[ProviderMessage]) -> str:
    if provider == "azure":
        return call_azure_openai(config, messages)
    elif provider == "ollama":
        return call_ollama(config, messages)
    else:
        return call_openrouter(config, messages)


def ask_schema_observer(question: str, dictionary: dict[str, Any]) -> str:
    """
    Main entry point for the Agentic Schema Observer.

    Injects the full Data Dictionary as context and routes
    the question to the configured LLM provider (BYOL principle).

    Testudo Formation: the LLM only sees data, never executes DDL.
    """
    provider, config = _get_provider_config()

    # Build context — inject the full dictionary as structured context
    dict_json = json.dumps(dictionary, indent=2, ensure_ascii=False)
    
    # Truncate if very large (keep first 12k chars = ~3k tokens)
    if len(dict_json) > 12000:
        dict_json = dict_json[:12000] + "\n... [dictionary truncated for context window]"

    user_message = f"""## User Question
{question}

## Data Dictionary (Schema Context)
```json
{dict_json}
```
"""

    messages = [
        ProviderMessage(role="system", content=SCHEMA_OBSERVER_SYSTEM_PROMPT),
        ProviderMessage(role="user", content=user_message),
    ]

    return _call_provider(provider, config, messages)
