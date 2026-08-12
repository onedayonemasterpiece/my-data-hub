# Lane oauth-client-disable-atomicity Results

## Status

Committed by the integration owner.

## Requirement IDs

- F5 residual: preserve OAuth client disable/revocation state atomically across startup.

## Delivered behavior

- Added a distinct static-configuration reconciliation statement that enables a
  client only on first insert and never updates the durable `enabled` security bit
  on conflict.
- Authorization-server startup no longer performs a read followed by a writable
  upsert, closing the race in which a concurrent administrative disable could be
  overwritten.
- The explicit administrative registration method retains its ability to enable or
  disable clients deliberately.

## Verification

- OAuth control/runtime focused tests: 5 passed.
- Ruff and `git diff --check`: passed.
- Tests prove metadata refresh preserves a disabled client and startup never calls
  the former read-before-upsert path.
