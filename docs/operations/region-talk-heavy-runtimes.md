# Region Talk heavy runtime contracts

Status: **contracted and offline-testable; not production-ready**.

This slice ports only the deterministic, evidence-checking core for the Region Talk
`image_scoring`, `final_verifier`, and `writer` stages. It does not add a scheduler,
SQL reader, provider client, network downloader, publication transport, or notification
transport. Every side effect is an injected protocol. An unattached required capability
raises retryable `HEAVY_RUNTIME_NOT_ATTACHED`; it never manufactures a successful result.

## Exact donor provenance

The reviewed donor is `events-bot-new@5bbdb681623d5e4e0bff2133e487a6663c1a838a`:

| Stage | Donor path | SHA-256 |
|---|---|---|
| image | `kaggle/RegionTalkImageDiagnostic/region_talk_image_diagnostic.py` | `d5e3683f04bab191b05881e3167b61b4a27b64eeef92c112548d054cbb245162` |
| verifier | `scripts/region_talk_publication_finalizer.py` | `1cf78a6ff6b2df21475587a83de1b4c4790080f55b84491939f96e9e8ab901fe` |
| writer | `scripts/region_talk_publication_draft_backfill.py` | `426d44396b04d7bff677497663a632219139ccfbe3354f31e99f4ecf38e0452a` |

The port preserves the reviewed score thresholds, low-score abstention, current-fact
grounding, source/externality checks, two-paragraph editorial audit, and a maximum of one
rewrite. Donor YDB writes, dynamic package/model downloads, global clients, Google calls,
and publication are deliberately excluded.

## Admission boundaries

- Pydantic models are frozen and `extra="forbid"`; stage/schema and work fingerprints
  must agree.
- All candidate, fact, source, media, typed upstream-result, and result hashes are bound
  to the current revision and recomputed where the full preimage is present.
- HTTPS source URLs reject credentials, non-default ports, localhost/private IP literals,
  and single-label hosts. A source URL is never an object locator.
- Image bytes are read only through a task-private `object_ref`. The typed immutable
  `region-talk-media-artifact-acquisition-receipt.v1` covers the exact ACTIVE epoch,
  accepted snapshot/stage run, candidate revision, content/asset identity, URL hash,
  byte hash/size/type/dimensions, and object reference. The injected reader must explicitly
  authorize that receipt hash. Mutable `task_readable` metadata is not an authority.
- Score/provider responses include exact model/request fingerprints. Result hashes are
  computed by the runtime, not accepted as free provider output.
- Verifier acceptance requires current fact IDs. Writer readiness requires a passed
  deterministic audit and critic, an accepted typed verifier result, selected media only,
  and no more than one rewrite.
- `publication_dispatch` and `notification_dispatch` are literal `false` in every input,
  receipt, result, and asset manifest.

## DAG bridge

`heavy_dag_bridge.py` parses the minimal migration-0030 work envelopes plus the immutable
acquisition receipt introduced by its authority follow-up, and derives the
closed `result_metadata.metrics` shapes checked by the SQL guard. It never enriches or
fetches evidence. Production wiring must obtain the richer closed input from authoritative
master views/receipts and prove the work-row fingerprint/revision before invoking a runtime.
A sparse 0030 envelope by itself is intentionally insufficient to execute a heavy stage.

## Offline assets and remaining prerequisites

`assets/heavy-runtime-assets.v1.json` disables network installs/downloads and is honestly
`production_ready=false`. The reviewed Kaggle CLIP provider reference exists, but its files
and dependency wheels are not pinned by locally verified SHA-256 values. LAION and NIMA
weights remain donor-dynamic/unresolved. The donor's `gemini-3.5-flash-lite` IDs have no
immutable provider revision and remain unresolved for all three remote-model roles. There is
no verified live smoke receipt. Therefore
production execution must remain retryable/blocked until an owner-reviewed bundle contains:

1. exact wheel filenames, versions, and hashes for the full dependency closure;
2. exact hashes/sizes for every model/config/tokenizer/quality-model file;
3. a matching immutable provider-image identity and runtime pin;
4. attached media/model/Google protocols and an evidence-backed smoke receipt.

No runtime is allowed to repair this gap by downloading packages or model files.
