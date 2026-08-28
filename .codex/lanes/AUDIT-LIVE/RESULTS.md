# Lane AUDIT-LIVE Results

## Status
committed

## Requirement IDs
- R1
- R6
- R7

## Branch
read-only runtime audit

## Worktree
`/home/dev/.codex/worktrees/my-data-hub/record-idea-hub-voice-intake`

## Base SHA
Deployed `5475ae68314be94fd473a7cd6db7f95eac519d44`.

## Head SHA
No code changes in this lane.

## Files changed
- `.codex/lanes/AUDIT-LIVE/RESULTS.md` (integrator-authored handoff receipt only)

## Commands run
- sanitized Docker/image/source attestation
- authenticated/unauthenticated health checks without revealing credentials
- read-only current IdeaHub card/head/blob resolution
- read-only deployed-code cache probe

## Tests / verification
The card was not embedded and was not loaded at startup. It was dynamically fetched after the image started. The deployed code used a process-global 300-second cache and silently returned stale content after refresh errors. Current card-changing commit was `f21f112703345df905ed47d507a7cecd0fca5923`; observed current blob was `8802bf33b5c0ff125cc2bf56607e9bcdcaaca69e`.

## Risks
Healthy containers alone do not prove a single snapshot across a long multi-chunk session; live publication acceptance remains required after the fix.

## Merge notes
No runtime state was mutated by the audit.
