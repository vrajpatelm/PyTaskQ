## What and why

## How it was tested
<!-- Unit tests, a real Redis, a real worker, a crash or load scenario? -->

## Checklist
- [ ] Tests for the change (`python -m pytest`)
- [ ] `mypy` and `ruff check --select E9,F63,F7,F82 pytaskq tests` pass (`pre-commit run --all-files`)
- [ ] Docs updated (API, ARCHITECTURE, GUARANTEES or the topic guide) and a `CHANGELOG.md` entry
- [ ] Redis data stays readable by the previous minor version (rolling upgrades)
