# Contributing to hale-bopp-db

Thanks for helping improve `hale-bopp-db`.

This project is meant to be understandable and usable by people outside the original EasyWay workspace, so contributions should optimize for clarity, reproducibility, and deterministic behavior.

## Code Of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).

## Before You Open A PR

- Check existing issues or discussions first
- Prefer small, reviewable pull requests
- Include tests when behavior changes
- Update docs when commands, API surface, or setup expectations change

## Branch And PR Flow

This repository uses a simple `feat/* -> main` flow.

1. Start from `main`
2. Create a descriptive branch such as `feat/column-rename-support` or `docs/readme-refresh`
3. Make your changes with tests
4. Open a pull request targeting `main`

Do not base work on `develop`; this repo does not use a `develop` branch.

## Local Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[api,dev]"
```

Optional local demo stack:

```bash
docker compose up --build
```

The compose stack is for local development only. Do not treat it as production infrastructure.

## Preferred Commands

```bash
hb --help
hb plan --connection <conn> --dictionary data/db-data-dictionary.json
hb drift --connection <conn> --dictionary data/db-data-dictionary.json
pytest tests/ -v
```

`halebopp` remains available as a legacy alias, but `hb` is the preferred CLI name in docs, templates, and examples.

## Commit Messages

Use clear conventional messages such as:

```text
feat(cli): add hb alias
fix(api): align health version metadata
docs(readme): clarify local demo vs production
```

## PR Checklist

- [ ] Tests pass locally
- [ ] No secrets or environment-specific credentials are committed
- [ ] README and CONTRIBUTING stay aligned with the real workflow
- [ ] PR description explains why the change matters
- [ ] The change keeps CLI, API, and packaging names coherent

## GitHub And Azure DevOps

The project is public on GitHub for discoverability and community signals.
EasyWay maintainers also track delivery through Azure DevOps internally.

For contributors, the important part is simple:

- branch from `main`
- open the PR against `main`
- keep the change self-contained and documented

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0 (see [LICENSE](LICENSE)).
