# Releasing PyTaskQ

Releases are automatic: push a version tag and GitHub Actions tests, builds and
publishes to PyPI, then creates a GitHub release with the files attached.

## One-time setup (about 5 minutes)

PyTaskQ uses **PyPI Trusted Publishing**. PyPI trusts this repository's
`release.yml` workflow directly, so there is no API token to create, store or leak.

1. **Create a PyPI account** at <https://pypi.org/account/register/> and turn on two-factor authentication (PyPI requires it).
2. **Add a pending publisher.** Go to <https://pypi.org/manage/account/publishing/> and fill in the GitHub section:

   | Field | Value |
   |---|---|
   | PyPI Project Name | `pytaskq` |
   | Owner | `vrajpatelm` |
   | Repository name | `PyTaskQ` |
   | Workflow name | `release.yml` |
   | Environment name | `pypi` |

   "Pending" means the project doesn't exist on PyPI yet. Your first release creates it and reserves the name.
3. **Create the `pypi` environment on GitHub.** In the repo, go to Settings → Environments → New environment → `pypi`.
   Recommended: under *Deployment protection rules*, add yourself as a **required reviewer**. Every release then waits for you to click "Approve" before anything is uploaded.

## Every release

```bash
# 1. Bump the version (single source of truth)
#    pytaskq/__init__.py  →  __version__ = "0.3.1"

# 2. Make sure everything passes locally
python -m pytest
cd frontend && npm run build && cd ..   # if the dashboard changed

# 3. Commit, tag, push
git commit -am "release: v0.3.1"
git tag v0.3.1
git push origin main --tags
```

Then watch the **Actions** tab. The `Release` workflow will:

1. run the test suite,
2. check the tag matches `__version__` (it stops if you forgot to bump),
3. build the wheel and sdist and validate them with `twine check`,
4. publish to PyPI (after your approval, if you set up a reviewer),
5. create a GitHub release with auto-generated notes and the built files.

A minute later: `pip install --upgrade pytaskq`.

## Versioning

Use [semantic versioning](https://semver.org/):

- `0.3.1`: bug fixes only
- `0.4.0`: new features, or changes users might need to react to (before 1.0)
- `1.0.0`: when you're ready to promise a stable API

PyPI never lets you re-upload the same version number, even after deleting it. If a release goes wrong, bump the patch version and tag again.

## Trying a release without PyPI

```bash
pip install build twine
python -m build              # creates dist/pytaskq-X.Y.Z-py3-none-any.whl and .tar.gz
twine check --strict dist/*
python -m venv /tmp/try && /tmp/try/bin/pip install dist/*.whl && /tmp/try/bin/pytaskq --help
```
