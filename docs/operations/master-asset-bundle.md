# Exact Kaggle master asset bundle

The control plane consumes a deterministic, secret-free release bundle instead of files
copied by hand. Build it only from a clean reviewed commit and place it outside the source
checkout:

```bash
.venv/bin/python scripts/provider/build_master_assets.py \
  --provider-owner zigomaro \
  --output /tmp/my-data-hub-master-assets-$(git rev-parse HEAD)
```

The command builds one wheel and packages the generated master Notebook, checkpoint
verifier Notebook, canonical `master-asset-bundle.json`, and `master-assets.env`. The
manifest binds every file hash to the exact Git commit and permanent owner/slug resource
identity. Directories are mode `0700`; files are mode `0600`. The environment file has
paths and identities only—never a Kaggle credential, callback token, database URL, or
session secret.

Copy the whole directory to the reviewed devstand release and set
`MY_DATA_HUB_MASTER_ASSET_DIR` to it. Do not publish the bundle as a public Dataset and do
not replace numeric provider versions or durable resource claims with `latest`. Runtime
callback authority is delivered through the separate task-owned private status Dataset;
it is not part of this static bundle.

Validate the manifest before install with
`schemas/master-asset-bundle.v1.schema.json`, then independently verify the exact approved
commit, canonical manifest, file inventory, modes, sizes, and hashes using only the host
Python standard library:

```bash
python3 scripts/provider/verify_master_assets.py \
  --bundle /path/to/master-assets \
  --expected-commit "$(git rev-parse HEAD)"
```

The committed example is synthetic contract documentation and is not live provider or
deployment evidence.
