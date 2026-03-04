# Contributing to HALE-BOPP

First off, thank you for considering contributing to HALE-BOPP! Every contribution
makes these tools better for everyone — from enterprise teams to small non-profit
associations that rely on open-source to manage their data.

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).
By participating, you are expected to uphold this code.

## How Can I Contribute?

### Reporting Bugs

- Use the **GitHub Issues** tab on the relevant repository
- Include: steps to reproduce, expected vs actual behavior, environment details
- Label with `bug` + the module (`db`, `etl`, `argos`)

### Suggesting Features

- Open a GitHub Issue with the `enhancement` label
- Describe the use case, not just the solution
- Reference relevant API contracts if applicable (see `API_CONTRACTS.md`)

### Submitting Code

1. **Fork** the repository
2. **Create a branch** from `develop`: `git checkout -b feat/your-feature develop`
3. **Write tests** — we aim for meaningful coverage, not 100% vanity metrics
4. **Follow existing patterns** — read the code before writing new code
5. **Commit** with clear messages: `feat(db): add column rename support`
6. **Open a PR** against `develop` (never directly to `main`)

### Commit Message Convention

```
<type>(<scope>): <description>

Types: feat, fix, docs, test, refactor, chore
Scopes: db, etl, argos, docs, ci
```

### PR Requirements

- [ ] Tests pass locally
- [ ] No hardcoded values (connection strings, passwords, paths)
- [ ] Documentation updated if API changes
- [ ] PR description explains **why**, not just **what**

## Development Setup

Each module has its own Docker Compose for local development:

```bash
# DB-HALE-BOPP
cd DB-HALE-BOPP
docker compose up

# ETL-HALE-BOPP
cd ETL-HALE-BOPP
docker compose up

# ARGOS-HALE-BOPP
cd ARGOS-HALE-BOPP
docker compose up
```

### Prerequisites

- Python 3.10+
- Docker & Docker Compose
- PostgreSQL (or use the Docker container)

## Architecture

HALE-BOPP follows the "Muscles" philosophy: **deterministic, no AI, pure mechanics**.

- `DB-HALE-BOPP` — Schema governance engine (port 8100)
- `ETL-HALE-BOPP` — Data orchestration (port 3000)
- `ARGOS-HALE-BOPP` — Quality gate policy engine (port 8200)

See `API_CONTRACTS.md` for the Universal Event Schema that ties them together.

## License

By contributing, you agree that your contributions will be licensed under the
Apache License 2.0 (see `LICENSE`).

## Questions?

Open a Discussion on GitHub or reach out via Issues. We're happy to help!
