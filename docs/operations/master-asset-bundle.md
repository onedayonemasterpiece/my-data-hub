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
  --embedding-wheelhouse /private/reviewed/embedding-worker-wheelhouse \
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

## Offline E5/BGE dependency closure

`scripts/provider/assets/embedding-worker-wheel-lock.v1.json` is the canonical source
contract for the small private overlay on the official Kaggle CPU v170 image. It binds
the PyPI simple index, exact `files.pythonhosted.org` wheel URLs, filenames, versions and
SHA-256 values for:

- FlagEmbedding 1.4.0 and its separately pinned `ir-datasets` runtime dependency;
- psycopg 3.3.4; and
- the matching psycopg-binary 3.3.4 CPython 3.12 manylinux x86-64 wheel.

The reviewed upstream records are the
[official Kaggle v170 CPU release](https://github.com/Kaggle/docker-python/releases/tag/v170-CPU-c1fa4de30bc268e601e6dcddb6ceb2519b9adde3527dbbfb05e6bdfbbbdcd1a2),
[FlagEmbedding 1.4.0 PyPI metadata](https://pypi.org/project/FlagEmbedding/1.4.0/),
and [Psycopg binary installation contract](https://www.psycopg.org/psycopg3/docs/basic/install.html#binary-installation).

Materialize those four files from the lock into a private staging directory. Do not add
the wheel bytes to Git, accept a source distribution, resolve `latest`, or add an
unreviewed file. The builder rejects a missing, extra, symlinked or hash-mismatched file,
then packages the exact files below `dataset/embedding-worker-wheelhouse/` and emits the
canonical `dataset/embedding-worker-dependencies.json`. The independent verifier repeats
the lock, inventory, size, hash, generated worker and smoke-runner checks. Neither command
downloads or imports the dependencies, and neither requires the roughly 9 GB official
image on the lightweight devstand.

The generated E5 and BGE workers validate both the dependency manifest and a central
provider-verified smoke receipt before installing anything. Each wheel is hash-checked
and installed individually with `pip --no-index --no-deps`; the project wheel uses the
same offline flags. These installs run before the embedded primary source can import E5,
BGE, psycopg, torch or transformers. A package index is never consulted at worker runtime.

`dataset/embedding-dependency-smoke.py` is a credential-free bounded runner for a
disposable private Notebook using the same exact asset Dataset and official v170 image.
It produces only an **observation** matching
`embedding-dependency-smoke-observation.v1.schema.json`: source commit, Python version,
hashes, import results, psycopg implementation and installed distribution versions. It
does not claim provider verification. The central Kaggle adapter must independently bind
the exact numeric private run, digest-pinned image, `enable_internet=false` launch setting
and observation into `embedding-dependency-smoke-receipt.v1.schema.json`.
The final receipt includes the SHA-256 of the exact canonical observation; this prevents
a central PASS from being detached from the provider-produced import evidence.

The normal asset build/install remains deployable without that provider run. Worker
admission does not: E5/BGE fail before dependency installation unless execution pins and
environment contain the exact receipt and manifest hashes and the receipt says
`verified_by_central_adapter=true`, private Notebook, internet disabled, exact v170 and
matching wheel inventory. The boolean is not an authorization signature by itself;
Gate K must admit only a receipt already verified and durably recorded by the central
adapter. Until that production wiring runs, embedding launches are intentionally blocked,
not reported as smoke-proven.

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
