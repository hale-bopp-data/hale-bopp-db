# hale-bopp-db

Deterministic schema governance engine for PostgreSQL.

Diff, deploy, and detect drift — no AI, no magic, just reliable mechanics.

## Features

- **Schema Diff**: Compare desired schema against actual database, get a change list with risk assessment
- **Schema Deploy**: Apply changes transactionally with automatic rollback on error
- **Drift Detection**: Detect unauthorized schema modifications vs baseline
- **CLI**: `halebopp diff`, `halebopp deploy`, `halebopp drift`, `halebopp snapshot`
- **REST API**: FastAPI endpoints for integration with orchestration tools

## Quick Start

```bash
# Docker Compose (includes PostgreSQL 16)
docker compose up

# Or install locally
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8100
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
pip install -r requirements.txt
pytest tests/ -v
```

## Part of HALE-BOPP

HALE-BOPP is an open-source ecosystem of deterministic data governance engines:

- **hale-bopp-db** (this repo) — Schema governance
- [hale-bopp-etl](https://github.com/hale-bopp-data/hale-bopp-etl) — Data orchestration
- [hale-bopp-argos](https://github.com/hale-bopp-data/hale-bopp-argos) — Policy gating

## License

Apache License 2.0 — see [LICENSE](LICENSE).
