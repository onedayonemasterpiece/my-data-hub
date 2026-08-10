#!/usr/bin/env python3
"""Offline validator for Kaggle canary receipts.

This lane intentionally contains no credential loading and no concrete Kaggle client.
The script validates receipts produced by a separately enabled canary adapter.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pydantic import ValidationError

from my_data_hub.providers.kaggle import DatasetCanaryReceipt, NotebookCanaryReceipt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate an exact private Kaggle canary receipt")
    parser.add_argument("kind", choices=("dataset", "notebook"))
    parser.add_argument("receipt", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = json.loads(args.receipt.read_text(encoding="utf-8"))
    model = DatasetCanaryReceipt if args.kind == "dataset" else NotebookCanaryReceipt
    try:
        receipt = model.model_validate(payload)
    except ValidationError as exc:
        print(json.dumps({"valid": False, "errors": exc.errors(include_url=False)}, default=str, sort_keys=True))
        return 1
    print(
        json.dumps(
            {
                "valid": True,
                "kind": args.kind,
                "canary_id": str(receipt.canary_id),
                "provider_ref": receipt.provider_ref,
                "privacy": receipt.privacy,
                "cleanup": receipt.cleanup.outcome,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
