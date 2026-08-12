"""Production construction for the lightweight connector intake service.

This process receives connector bearer secrets, the control ledger, and bounded
epoch credential documents. It never receives a static database URL or stores
canonical data locally.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI

from my_data_hub.api.app import create_app
from my_data_hub.config import ConfigurationError, Settings
from my_data_hub.connectors.durability import (
    VerifiedCheckpointCoordinator,
    build_connector_checkpoint_gateway,
)
from my_data_hub.connectors.runtime import (
    ActiveMasterConnectorDurabilityRuntime,
    ActiveMasterConnectorRuntime,
    DirectoryConnectorDurabilitySessionBroker,
    DirectoryConnectorSessionBroker,
)
from my_data_hub.control_plane.adapters import LedgerMasterResolver
from my_data_hub.control_plane.app import assert_no_database_environment
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.mcp.postgres_broker import DirectoryEpochCredentialSource


@dataclass(frozen=True, slots=True)
class ConnectorAPIRuntime:
    settings: Settings
    ledger: ControlLedger
    app: FastAPI


def build_connector_api_runtime(
    *,
    settings: Settings | None = None,
    ledger: ControlLedger | None = None,
    checkpoint_coordinator: VerifiedCheckpointCoordinator | None = None,
) -> ConnectorAPIRuntime:
    """Build connector intake around exact injected master dependencies.

    ``checkpoint_coordinator`` intentionally has no environment-driven fallback:
    the general master checkpoint owner must inject the exact callable contract.
    Until it does, authenticated submissions return a precise pre-mutation 503.
    """

    assert_no_database_environment()
    runtime_settings = settings or Settings.from_env(require_database=False)
    if any(
        (
            runtime_settings.database_url,
            runtime_settings.application_database_url,
            runtime_settings.connector_intake_database_url,
            runtime_settings.orchestrator_database_url,
            runtime_settings.canonical_committer_database_url,
        )
    ):
        raise ConfigurationError(
            "connector intake process must not receive a static database URL"
        )
    if runtime_settings.environment in {"prod", "production"} and not (
        runtime_settings.connector_credentials
    ):
        raise ConfigurationError(
            "production connector intake requires connector bearer credentials"
        )
    control_ledger = ledger or ControlLedger(
        Path(
            os.getenv("MY_DATA_HUB_CONTROL_LEDGER_PATH", "/ledger/control.sqlite3")
        ).expanduser()
    )
    resolver = LedgerMasterResolver(control_ledger)
    credentials = DirectoryEpochCredentialSource(
        Path(
            os.getenv(
                "MY_DATA_HUB_MASTER_SESSION_DIRECTORY", "/sessions"
            )
        ).expanduser()
    )
    gateway = build_connector_checkpoint_gateway(checkpoint_coordinator)
    intake_runtime = ActiveMasterConnectorRuntime(
        resolver=resolver,
        broker=DirectoryConnectorSessionBroker(
            credentials, max_envelope_bytes=runtime_settings.connector_intake_max_bytes
        ),
        max_envelope_bytes=runtime_settings.connector_intake_max_bytes,
    )
    durability_runtime = ActiveMasterConnectorDurabilityRuntime(
        resolver=resolver,
        broker=DirectoryConnectorDurabilitySessionBroker(credentials, gateway),
        checkpoint_gateway=gateway,
    )
    app = create_app(
        runtime_settings,
        connector_runtime=intake_runtime,
        connector_durability_runtime=durability_runtime,
        worker_results_enabled=False,
    )
    return ConnectorAPIRuntime(runtime_settings, control_ledger, app)


def serve(*, checkpoint_coordinator: Any | None = None) -> None:
    try:
        import uvicorn
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("uvicorn is required to run connector intake") from exc
    runtime = build_connector_api_runtime(checkpoint_coordinator=checkpoint_coordinator)
    uvicorn.run(
        runtime.app,
        host=runtime.settings.api_host,
        port=runtime.settings.api_port,
        log_level=runtime.settings.log_level.lower(),
        proxy_headers=False,
        server_header=False,
    )


if __name__ == "__main__":  # pragma: no cover
    serve()
