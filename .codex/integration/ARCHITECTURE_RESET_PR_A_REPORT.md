# PR-A integration report

- Base: `bcc02df1f980ac6eefcd305d71cef94817033d70`
- Branch: `integration/architecture-reset-pr-a`
- Scope: architecture correction and deployment-path containment only
- Forbidden operations executed: none

## Requirement closure before independent review

| ID | Status | Evidence |
|---|---|---|
| R01 | Done with disclosed residue | `docs/operations/evidence/2026-08-10-pr-a-host.json`; no runtime/container/listener/unit, but an unattached empty validation volume object exists and was not deleted |
| R02 | Done | incident record, ADR-0016 and exact-source SHA binding |
| R03 | Done | `architecture/invariants.yaml` plus hard-coded validator expectations |
| R04 | Done | ADR-0009 superseded; dependent ADRs and listed documentation rebound |
| R05 | Done | old token exits 78 before side effects; production profile has one DB-free control service; legacy deployment paths removed |
| R06 | Done | topology documents specify Kaggle master, private checkpoint generations, lightweight devstand and direct data plane |
| R07 | Done | `docs/roadmap-architecture-reset.md` orders FakeKaggle/runtime work before master PoC and keeps Region Talk last |
| R08 | Done | semantic architecture, deployment and `master=ABSENT` tests |
| R09 | Done | `docs/architecture/work-preservation-map.md` |
| R10 | Pending | requires PR, green hosted CI, independent XHigh review and merge |

## Local verification

At the implementation head before review:

- `python -m compileall -q src tests scripts`: PASS
- `ruff check .`: PASS
- `pytest -q`: PASS, 242 tests
- `python scripts/validate_repository.py`: PASS, 2389 checks / 0 errors
- `python scripts/create_notebooks.py --check`: PASS, no drift
- integration and control Compose parsing: PASS; neither declares named volumes
- `bash -n deploy/same-host/install.sh deploy/control-plane/install.sh`: PASS
- `git diff --check`: PASS

## Safety and deferred work

The rejected same-host install token was not run. Neither the replacement control-plane
installer nor any production deployment was run. No local PostgreSQL process, initialized
PGDATA, migration, backup, DNS/VPN change, remote MCP write, Kaggle master, or Region Talk
migration was created by PR-A. PR-B is explicitly out of scope.

## Independent review remediation

The first XHigh pass found contradictory preserved prose, a narrow drift scan and an
incomplete control-plane credential denylist. The same PR now:

- binds orchestrator availability, security trust zones, post-deploy/nightly evidence and
  Region Talk readiness to the control-plane/Kaggle-master split;
- inventories every Compose/deploy/workflow surface and scans all executable-shaped files
  with pattern/path/line-specific occurrence multisets in addition to semantic document
  checks; any extra or rewritten matching command fails;
- discovers both YAML suffixes, Compose and Docker-Compose filename families, arbitrary YAML
  service documents and executable Python scripts rather than relying on one filename glob;
- rejects every known database credential variable, standard libpq connection variables
  (including future `PG*` additions) and any future `*_DATABASE_URL` in the DB-free control
  process.

An exact-head re-review is required before merge.
