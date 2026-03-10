# hale-bopp-db

[![CI](https://github.com/hale-bopp-data/hale-bopp-db/actions/workflows/ci.yml/badge.svg)](https://github.com/hale-bopp-data/hale-bopp-db/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.10+-green.svg)](https://python.org)

Deterministic schema governance engine for PostgreSQL.

Diff, deploy, and detect drift — no AI, no magic, just reliable mechanics.

## Architecture

```
  ┌─────────────┐         ┌──────────────────┐         ┌──────────────┐
  │ Desired     │         │   hale-bopp-db   │         │  PostgreSQL  │
  │ Schema JSON │────────►│                  │────────►│  Database    │
  └─────────────┘  diff   │  :8100           │ deploy  └──────────────┘
                          │                  │◄────────
                          │                  │  drift    ┌──────────────┐
                          │                  │◄────────  │  Baseline    │
                          └──────────────────┘  check   │  Snapshot    │
                                                        └──────────────┘
```

## Features

- **Schema Diff**: Compare desired schema against actual database, get a change list with risk assessment
- **Schema Deploy**: Apply changes transactionally with automatic rollback on error
- **Drift Detection**: Detect unauthorized schema modifications vs baseline
- **CLI**: `halebopp diff`, `halebopp deploy`, `halebopp drift`, `halebopp snapshot`
- **REST API**: FastAPI endpoints for integration with orchestration tools
- **Risk Assessment**: Every change gets a risk level (low/medium/high/critical)

## Installation

```bash
# From PyPI (when published)
pip install hale-bopp-db

# From source
pip install git+https://github.com/hale-bopp-data/hale-bopp-db.git

# With REST API support
pip install hale-bopp-db[api]

# Development
git clone https://github.com/hale-bopp-data/hale-bopp-db.git
cd hale-bopp-db
pip install -e ".[dev,api]"
```

## Quick Start

```bash
# Use the CLI
halebopp snapshot --connection postgresql://user:pass@localhost/mydb -o baseline.json
halebopp diff --connection postgresql://user:pass@localhost/mydb --desired schema.json

# Start the API server
uvicorn hale_bopp_db.main:app --host 0.0.0.0 --port 8100

# Or use Docker Compose (includes PostgreSQL 16)
docker compose up
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/schema/diff` | Calculate schema differences + risk level |
| `POST` | `/api/v1/schema/deploy` | Apply changes (supports dry-run) |
| `POST` | `/api/v1/drift/check` | Detect unauthorized schema drift |
| `GET` | `/api/v1/health` | Service health check |

## CLI

```bash
halebopp diff --connection <conn> --desired schema.json
halebopp deploy --connection <conn> --changes changes.json [--execute]
halebopp drift --connection <conn> --baseline baseline.json
halebopp snapshot --connection <conn> -o baseline.json
```

## Testing

```bash
pip install -e ".[dev,api]"
pytest tests/ -v
```

17 tests covering diff engine, deploy logic, drift detection, CLI, and API endpoints.

## Part of HALE-BOPP

> *Sovereign by design. Cloud by choice.*

HALE-BOPP is an open-source ecosystem of deterministic data engines — the "muscles" that do the heavy lifting, no AI required. Portable, replicable, and sovereign: your data governance runs where you decide, not where a vendor tells you.

```
  ┌──────────┐     event      ┌──────────┐     gate      ┌──────────┐
  │ DB :8100 │ ─────────────► │ETL :3001 │ ◄──────────── │ARGOS:8200│
  │ schema   │                │ pipeline │               │ policy   │
  │ govern.  │                │ runner   │               │ gating   │
  └──────────┘                └──────────┘               └──────────┘
```

- **hale-bopp-db** (this repo) — Schema governance for PostgreSQL
- [hale-bopp-etl](https://github.com/hale-bopp-data/hale-bopp-etl) — Config-driven data orchestration
- [hale-bopp-argos](https://github.com/hale-bopp-data/hale-bopp-argos) — Policy gating and quality checks
- [marginalia](https://github.com/hale-bopp-data/marginalia) — Markdown vault quality scanner

## License

Apache License 2.0 — see [LICENSE](LICENSE).
