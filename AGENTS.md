---
title: "Agents Master"
tags: []
---

# AGENTS.md — hale-bopp-db

> Motore deterministico per schema governance: diff, deploy, drift detection.
> Guardrails e regole: vedi `.cursorrules` nello stesso repo.
> Workspace map: vedi `factory.yml` nella root workspace (mappa completa repos, stack, deploy).

## Identità
| Campo | Valore |
|---|---|
| Cosa | Python app — schema diff, deploy, drift detection per PostgreSQL |
| Linguaggio | Python 3.11, SQL, Docker |
| Branch | `feat→main` (NO develop) — PR target: `main` |
- **Container**: `halebopp-db`
- **Tests**: 17

## Comandi rapidi
```bash
ewctl commit
# Run tests
pytest app/tests/
# Docker build + run
docker compose up -d
# Schema diff
python -m app.cli diff
```

## Struttura
```text
app/
  cli.py             # CLI entry point
  core/              # Schema diff engine
  models/            # Data models
  api/               # REST API
dist/                # Distribution artifacts
docs/                # Documentation
docker-compose.yml   # Dev environment
Dockerfile           # Container image
```

## Regole specifiche hale-bopp-db
| Regola | Dettaglio |
|---|---|
| Determinismo | stesso input = stesso output |
| Test | `pytest` con coverage |
| Docker | pin versions, multi-stage build |

## Workflow & Connessioni
| Cosa | Dove |
|---|---|
| ADO operations (WI, PR) | → vedi `easyway-wiki/guides/agents/agent-ado-operations.md` |
| PR flusso standard | → vedi `easyway-wiki/guides/polyrepo-git-workflow.md` |
| PAT/secrets/gateway | → vedi `easyway-wiki/guides/connection-registry.md` |
| Branch strategy | → vedi `easyway-wiki/guides/branch-strategy-config.md` |
| Tool unico | `bash /c/old/easyway/agents/scripts/connections/ado.sh` — MAI curl inline, MAI az login |


---
> Context Sync Engine | Master: `easyway-wiki/templates/agents-master.md`
> Override: `easyway-wiki/templates/repo-overrides.yml` | Sync: 2026-03-21T12:00:10Z
