# hale-bopp-db

[![CI](https://github.com/hale-bopp-data/hale-bopp-db/actions/workflows/ci.yml/badge.svg)](https://github.com/hale-bopp-data/hale-bopp-db/actions/workflows/ci.yml)
[![Secrets Scan](https://github.com/hale-bopp-data/hale-bopp-db/actions/workflows/secrets-scan.yml/badge.svg)](https://github.com/hale-bopp-data/hale-bopp-db/actions/workflows/secrets-scan.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.10%2B-green.svg)](https://python.org)

Deterministic schema governance for PostgreSQL.

`hale-bopp-db` compares a desired schema or data dictionary with a live database, produces a reviewable plan, applies changes transactionally, and exposes the same engine through a CLI and a FastAPI API.

## What It Does

- Diff desired schema vs live PostgreSQL
- Build reviewable plans before execution
- Apply schema changes transactionally
- Detect drift against a baseline or data dictionary
- Generate docs and ER diagrams from the dictionary
- Expose the same capabilities via CLI and REST API

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[api,dev]"

hb --help
uvicorn app.main:app --reload --host 0.0.0.0 --port 8100
```

Open:

- API health: `http://localhost:8100/api/v1/health`
- Interactive console: `http://localhost:8100/console`

## CLI

Preferred command:

```bash
hb --version
hb diff --connection <conn> --desired schema.json
hb plan --connection <conn> --dictionary data/db-data-dictionary.json
hb apply --connection <conn> --plan halebopp-plan.json --execute
hb drift --connection <conn> --dictionary data/db-data-dictionary.json
hb docs generate --dictionary data/db-data-dictionary.json --output dist/docs
```

Backward-compatible alias:

```bash
halebopp --help
```

## API Surface

Representative endpoints:

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/health` | Health + package version |
| `GET` | `/api/v1/dictionary` | Serve the default data dictionary |
| `POST` | `/api/v1/plan` | Create a reviewable schema plan |
| `POST` | `/api/v1/apply` | Apply a plan to a live database |
| `POST` | `/api/v1/drift/dictionary` | Detect drift against the data dictionary |
| `POST` | `/api/v1/compile` | Compile the dictionary into DDL/config artifacts |
| `POST` | `/api/v1/validate` | Run structural and security validation checks |
| `POST` | `/api/v1/schema/maetel` | Generate ER output from a live database |

## Local Demo Vs Production

`docker-compose.yml` is for local demo and development only. It spins up a disposable PostgreSQL instance with demo credentials so you can exercise the API and CLI quickly.

Use it for:

- local feature development
- smoke testing the API
- trying the console and dictionary workflows

Do not treat it as a production deployment:

- demo credentials are intentionally local-only
- there is no hardened secret management
- persistence and networking are tuned for convenience, not operations
- production rollouts should use environment-specific configuration, managed secrets, backups, and your own deployment automation

Copy [.env.example](.env.example) when preparing local settings and replace placeholder values before any serious environment.

## CI And Release Signals

- Public GitHub Actions workflows live in [`.github/workflows`](.github/workflows)
- Azure Pipelines remains available for the internal EasyWay delivery flow
- The README badge points to the canonical GitHub CI workflow so the repo state is discoverable at a glance

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for branch, testing, and PR expectations.

## Part Of HALE-BOPP

HALE-BOPP is an open-source ecosystem of deterministic data engines:

- `hale-bopp-db` - schema governance for PostgreSQL
- [hale-bopp-etl](https://github.com/hale-bopp-data/hale-bopp-etl) - config-driven data orchestration
- [hale-bopp-argos](https://github.com/hale-bopp-data/hale-bopp-argos) - policy gating and quality checks

## License

Apache License 2.0 - see [LICENSE](LICENSE).
