"""Verified physical/logical checkpoint contracts and promotion core."""

from .manifest import CheckpointManifest, ManifestError, build_manifest, load_and_verify
from .publisher import CheckpointPublisher, PublishError
from .registry import CheckpointHead, CheckpointRegistry, CheckpointStatus
from .restore import PhysicalRestoreError, restore_physical_archive
from .restore_probe import collect_restore_probe, logical_probe_hash

__all__ = [
    "CheckpointHead",
    "CheckpointManifest",
    "CheckpointPublisher",
    "CheckpointRegistry",
    "CheckpointStatus",
    "ManifestError",
    "PhysicalRestoreError",
    "PublishError",
    "build_manifest",
    "collect_restore_probe",
    "load_and_verify",
    "logical_probe_hash",
    "restore_physical_archive",
]
