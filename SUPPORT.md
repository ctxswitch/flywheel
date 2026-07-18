# Support

Flywheel is maintained as an open-source project without a support service-level agreement.

## Before opening an issue

1. Check [README.md](README.md), [BUILDING.md](BUILDING.md), and
   [docs/operations.md](docs/operations.md).
2. Search existing issues for the error, client, or deployment pattern.
3. Confirm the problem on the latest release or current `main` when practical.

## Bug reports

Open a GitHub issue with:

- Flywheel version or commit.
- Operating system, architecture, and deployment topology.
- Relevant configuration with tokens and credentials removed.
- Exact reproduction steps and expected behavior.
- Logs around the failure and whether it survives restart.
- For Kubernetes problems, chart version and relevant values or rendered resource fragments.

Use a minimal reproducer whenever possible. Do not attach cache data or logs that contain private
package names, source URLs, authorization headers, or channel tokens.

## Feature requests and questions

Open an issue describing the workload, current limitation, desired outcome, and operational
constraints. Large changes should establish the problem and compatibility requirements before an
implementation pull request is opened.

## Security and conduct

- Report vulnerabilities through [SECURITY.md](SECURITY.md).
- Report community conduct incidents through [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

Neither category should be reported in a public issue.
