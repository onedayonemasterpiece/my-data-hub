# EMBEDDING-PUSH-INTEGRITY results

- Rendered disposable worker source embeds the complete task UUID literal and rejects status metadata with a different task identity before attestation or direct-data-plane setup.
- A focused test exercises the real `KaggleProviderAdapter` pre-provider UUID contract: source without the literal is rejected without a provider call; rendered source is accepted.
- Restarted `ACCESS_READY` launches reconcile the deterministic status create without reissuing access.
- `STATUS_CREATED` or cleanup-in-progress launches cannot push again. Cleanup durably revokes partial access and deletes the exact status claim; response-loss replay is idempotent across a fresh launcher.
- Durable journal remains metadata-only; capability secrets are removed only after cleanup completes.

No live provider or deployment mutation was performed.
