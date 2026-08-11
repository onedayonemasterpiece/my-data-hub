"""Source template entry point for the protected PostgreSQL master Notebook.

The notebook generator embeds this source.  Runtime secrets are read only from the
Kaggle secret environment and are never emitted in notebook outputs.
"""

from __future__ import annotations

import os
from pathlib import Path

from my_data_hub.master_runtime.contracts import MasterPaths


def working_layout() -> MasterPaths:
    working = Path(os.environ.get("KAGGLE_WORKING_DIR", "/kaggle/working"))
    paths = MasterPaths.under(working)
    paths.validate()
    return paths


def required_secret(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"required Kaggle secret is absent: {name}")
    return value
