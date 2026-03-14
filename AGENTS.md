---
title: "Agents Master"
tags: []
---

# AGENTS.md — hale-bopp-db

> Motore deterministico per schema governance: diff, deploy, drift detection.
> Guardrails e regole: vedi `.cursorrules` nello stesso repo.

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

## ADO Workflow
```bash
# Tool UNICO — MAI curl inline, MAI az login
bash /c/old/easyway/ado/scripts/ado-remote.sh wi-create "titolo" "PBI" "tag1;tag2"
bash /c/old/easyway/ado/scripts/ado-remote.sh pr-create hale-bopp-db <src> main "AB#NNN titolo" NNN
bash /c/old/easyway/ado/scripts/ado-remote.sh pr-autolink-wi <pr_id> hale-bopp-db
bash /c/old/easyway/ado/scripts/ado-remote.sh pat-health-check
```
Repo ADO: `easyway-portal`, `easyway-wiki`, `easyway-agents`, `easyway-infra`, `easyway-ado`, `easyway-n8n`

## PR — Flusso standard
```bash
cd /c/old/hale-bopp/db && git push -u origin feat/nome-descrittivo
bash /c/old/easyway/ado/scripts/ado-remote.sh pr-create hale-bopp-db feat/nome-descrittivo main "AB#NNN titolo" NNN
```


## Connessioni
- **PAT/secrets**: SOLO su server `/opt/easyway/.env.secrets` — MAI in locale
- **Guida**: `easyway-wiki/guides/connection-registry.md`
- **`.env.local`**: solo OPENROUTER_API_KEY e QDRANT

---
> Context Sync Engine | Master: `easyway-wiki/templates/agents-master.md`
> Override: `easyway-wiki/templates/repo-overrides.yml` | Sync: 2026-03-14T21:00:06Z
