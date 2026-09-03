# Owner-hosted GitHub Actions runners are prohibited

Date: 2026-09-03
Status: ACTIVE OWNER DECISION

No GitHub Actions self-hosted runner may be installed, registered, enabled, started, restarted, recovered, or used on DevCoveer or another owner-controlled machine without a new explicit owner authorization.

Repository workflows containing `runs-on: self-hosted` are not installation authority. They are non-operational until redesigned or separately approved. Agents must not solve a queued workflow by installing software on the owner's machine.

Dataset Loop MCP must use a deployment execution path that does not require an owner-hosted Actions runner. The former runner recovery script is intentionally fail-closed, and the Dataset Loop self-hosted deployment/probe workflows have been removed or disabled.
