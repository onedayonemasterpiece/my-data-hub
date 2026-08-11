# Exact Kaggle master asset bundle

The control plane consumes a deterministic, secret-free release bundle instead of files
copied by hand. Build it only from a clean reviewed commit and place it outside the source
checkout:

```bash
.venv/bin/python scripts/provider/build_master_assets.py \
  --provider-owner zigomaro \
  --postgres-runtime-archive /private/reviewed/postgresql-18-runtime.tar.gz \
  --postgres-runtime-sha256 40bf34fb4a97a248537d0221127e38deb98c9b35208d474dd1b93f773c2558b5 \
  --tunnel-known-hosts /private/reviewed/hashed-known-hosts \
  --output /tmp/my-data-hub-master-assets-$(git rev-parse HEAD)
```

The command builds one wheel and packages the generated master Notebook, checkpoint
verifier Notebook, the reviewed PostgreSQL 18.4 + pgvector 0.8.6 portable runtime,
its canonical provenance manifest, a hashed reviewed tunnel host key, canonical
`master-asset-bundle.json`, and `master-assets.env`. The PostgreSQL artifact is built by
`scripts/provider/assets/postgresql-18.4-pgvector-0.8.6.Dockerfile`: the base image is
digest-pinned, both upstream archives are SHA-256 pinned, pgvector disables host-native
`OPTFLAGS`, and the output tar/gzip metadata is deterministic. The artifact bytes stay
outside Git and must match the explicit CLI digest; the known-host file must already use
OpenSSH hashed-host syntax.

The reviewed local input may retain its `.tar.gz` name, but the Dataset package stores
the exact same bytes as `dataset/postgresql-18-runtime.bundle`. A real Kaggle upload on
2026-08-11 proved that a `.tar.gz` Dataset member is provider-expanded and therefore no
longer has the exact staged package tree; the neutral `.bundle` suffix preserves the
single opaque artifact and its approved SHA-256. The Notebook still verifies that hash
before passing the bytes to `tar -xzf`.

The
manifest binds every file hash to the exact Git commit and permanent owner/slug resource
identity. Directories are mode `0700`; files are mode `0600`. The environment file has
paths and identities only—never a Kaggle credential, callback token, database URL, or
session secret.

Copy the whole directory to the reviewed devstand release and set
`MY_DATA_HUB_MASTER_ASSET_DIR` to it. Do not publish the bundle as a public Dataset and do
not replace numeric provider versions or durable resource claims with `latest`. Runtime
callback authority is delivered through the separate task-owned private status Dataset;
it is not part of this static bundle.

For each admitted attempt the control plane adds a secret-free `master-config.json` to
that protected status Dataset. It binds either an EMPTY boot or the exact current
VERIFIED checkpoint id, HEAD generation, numeric Dataset version, and manifest hash.
The exact checkpoint version is attached to the Notebook and compared again immediately
before push. Control generates a task/operation/epoch-bound PostgreSQL certificate and
private key and puts the raw bytes only in the same exact protected status Dataset. The
bootstrap hash-validates and writes both to fixed mode-`0600` files below
`/kaggle/working`; neither the private key nor callback token enters source, the static
asset Dataset, ledger, receipts, or logs.

These assets are consumed by the brokered checkpoint path. The Notebook contains no
Kaggle SDK credential and performs only direct HTTPS PUTs to one-file signed upload URLs.
The central adapter retains every opaque blob token, finalizes the exact private Dataset
version, launches the independently pinned verifier, and advances HEAD only after the
verified restore receipt. Missing broker key/configuration fails before master launch with
`checkpoint_upload_broker_unavailable`.

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
