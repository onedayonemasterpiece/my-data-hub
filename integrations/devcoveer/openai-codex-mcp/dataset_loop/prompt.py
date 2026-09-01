from __future__ import annotations

import re
from collections.abc import Iterable

_RUN_ID = re.compile(r"run_[0-9a-f]{32}")


def render_canonical_prompt(*, run_id: str, frozen_dataset_ids: Iterable[str], **_contexts: object) -> str:
    """Model and logical context deliberately never change prompt bytes."""
    if _RUN_ID.fullmatch(run_id) is None:
        raise ValueError("invalid run id")
    frozen = ",".join(sorted(str(value) for value in frozen_dataset_ids))
    return (
        "DevCoveer dataset loop.\n"
        f"run_id={run_id}\n"
        f"frozen_dataset_ids={frozen}\n"
        "Use only frozen inputs. Emit typed research results; do not mutate Git.\n"
    )
