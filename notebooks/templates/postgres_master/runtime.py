"""Primary source template for the protected PostgreSQL master Notebook."""

from __future__ import annotations

from my_data_hub.master_runtime.notebook_entrypoint import main as run_notebook_master


def main() -> int:
    return run_notebook_master()
