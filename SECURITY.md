# Security Policy

## Supported versions

Security fixes are applied to the latest release and `main`. Older releases are not maintained
unless the maintainers announce an exception.

| Version | Security fixes |
| --- | --- |
| Latest release | Yes |
| `main` | Yes |
| Older releases | No |

## Report a vulnerability

Do not disclose a suspected vulnerability in a public issue, pull request, discussion, or chat.
Use one of these private channels:

1. Open a [private GitHub security
   advisory](https://github.com/ctxswitch/flywheel/security/advisories/new).
2. If private reporting is unavailable, email [rob@ctxswitch.com](mailto:rob@ctxswitch.com) with
   the subject `Flywheel security report`.

Include the affected version or commit, deployment conditions, reproduction steps, impact, and any
known mitigations. Remove credentials, channel tokens, and unrelated customer data from the
report.

Maintainers will acknowledge the report, investigate it, and coordinate disclosure and a fix with
the reporter. Response and release timing depend on severity and maintainer availability; no
service-level agreement is provided.

## Security boundary

Flywheel is a cache, not a system of record. It serves plain HTTP and expects TLS termination,
network access control, and registry-level administration at the deployment boundary. Protected
channel tokens authorize all operations in their channel and must be treated as secrets.

The operational security model and hardening guidance are documented in
[docs/operations.md](docs/operations.md).
