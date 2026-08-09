from __future__ import annotations

import json
import signal
import threading
from datetime import UTC, datetime

from my_data_hub.orchestrator.cycle import record_region_talk_plan


def heartbeat(database_url: str, instance_id: str, *, scheduler_enabled: bool) -> dict[str, object]:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg is required") from exc

    event: dict[str, object] = {
        "component": "orchestrator",
        "instance_id": instance_id,
        "scheduler_enabled": scheduler_enabled,
        "at": datetime.now(UTC).isoformat(),
    }
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT canonical_revision, schema_revision "
                "FROM hub.canonical_state WHERE singleton = true"
            )
            row = cursor.fetchone()
            event["canonical_revision"] = int(row[0]) if row else None
            event["schema_revision"] = int(row[1]) if row else None
            cursor.execute(
                """
                INSERT INTO sync.audit_event (actor_id, client_id, action, outcome, details)
                VALUES ('system', %s, 'orchestrator.heartbeat', 'ok', %s::jsonb)
                """,
                (instance_id, json.dumps(event)),
            )
        connection.commit()
    return event


def run_loop(
    database_url: str,
    instance_id: str,
    *,
    interval_seconds: int = 60,
    scheduler_enabled: bool = False,
    max_actions: int = 8,
) -> None:
    """Run supervised short cycles and stop cleanly on SIGINT/SIGTERM.

    The bootstrap loop records plans only. Provider launch, result application and every
    external side effect remain separate, explicitly enabled adapters.
    """
    stop = threading.Event()

    def request_stop(_signum: int, _frame: object) -> None:
        stop.set()

    previous: dict[int, object] = {}
    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.signal(signum, request_stop)
    try:
        while not stop.is_set():
            event = heartbeat(database_url, instance_id, scheduler_enabled=scheduler_enabled)
            if scheduler_enabled:
                try:
                    event["cycle"] = record_region_talk_plan(
                        database_url,
                        trigger={"kind": "local-loop", "instance_id": instance_id},
                        max_actions=max_actions,
                    )
                except Exception as exc:  # keep supervisor alive; failure remains visible
                    event["cycle_error"] = {
                        "type": type(exc).__name__,
                        "message": str(exc),
                    }
            print(json.dumps(event, ensure_ascii=False, default=str), flush=True)
            stop.wait(interval_seconds)
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)  # type: ignore[arg-type]
