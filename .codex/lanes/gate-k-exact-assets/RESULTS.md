# Gate K exact asset assembly results

- Verified bundle now pins the reviewed official Kaggle v170 CPU image digest, release URL, source commit, Python series, exact wheel hash/path, and generated E5/BGE worker assets.
- The single central adapter writes `docker_image` with pinning type `original` for master and disposable embedding Notebook pushes; ordinary pushes and Dataset effects are unchanged.
- Master execution pins are generated only after the exact asset Dataset claim exists and are embedded/hash-bound in the pushed source. Embedding pins are task-owned files in the private disposable status Dataset.
- Embedding workers authenticate source/image/source-commit/epoch with their private task token before any direct database access. Cleanup remains task-claim-bound and restart safe.
- Embedding assembly is cold-start safe: the app remains available without an asset claim and activates only after the same adapter's deterministic asset effect and receipt are durably cross-bound to the exact claim.

Validation: full pytest, focused tests, ruff, compileall, generated-notebook drift check, and repository validator passed. No live provider or deployment mutation was performed.
