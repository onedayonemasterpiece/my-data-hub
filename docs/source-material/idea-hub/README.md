# Exact target-vision import gate

The canonical source is not reconstructed from memory or from normalized documents.
An authenticated clone of `onedayonemasterpiece/idea-hub` must be available locally,
then the exact Git object is imported with:

```bash
python scripts/import_source_material.py --source-repo /path/to/idea-hub
```

The command extracts commit `0c3fcf7` and path
`ideas/portfolio.inbox/idea-20260809-content-platform-current-design.md`, writes the
exact bytes to this directory, verifies SHA-256 and changes the provenance status to
`verified_import`. Until that succeeds, the normalized architecture is usable as a
bootstrap but the exact source-material gate remains open.
