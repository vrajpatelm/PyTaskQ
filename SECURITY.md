# Security policy

## Supported versions

Security fixes go into the latest minor release only.

| Version | Supported |
|---|---|
| 0.6.x | Yes |
| < 0.6 | No — upgrade ([docs/UPGRADING.md](docs/UPGRADING.md)) |

## Reporting a vulnerability

**Please don't open a public issue.** Report it privately through GitHub: on the repository's
**Security** tab, choose **Report a vulnerability**. Include what an attacker can do, the steps to
reproduce it, and the version you tested.

You'll get an answer within 7 days. Once a fix is released, the advisory is published with credit
to you, unless you'd rather stay anonymous.

## In scope

- The dashboard and REST API: sign-in, sessions, API tokens, roles, invites, CSRF and CORS.
- Job handling that could run, read or change another user's jobs.
- The webhook and URL fetching guard (`pytaskq.net`).
- The Docker image and the production setup in `deploy/`.

Out of scope: running with `PYTASKQ_AUTH=off`, a Redis that is reachable without a password, and
task code you write yourself. [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) has the security checklist
for a production server.
