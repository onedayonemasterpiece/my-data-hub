# Retired Yandex public edge

This directory is retained only as historical evidence of the 2026-08-11 deployment.
`provision.sh` is an unconditional fail-closed tombstone and cannot create/reconcile cloud
resources. The owner decision of 2026-08-13 places the public edge on DevCoveer.

Decommissioning is a live operational procedure, not a folder-wide cleanup script. It may
touch only the exact task-owned `my-data-hub public-edge` manifest after the local ingress,
DNS, client, VPN, reboot, renewal and rollback gates pass. The shared DNS zone, Object
Storage buckets, CDN, static-site certificates, Postbox/mail infrastructure, Identity Hub,
YDB and unlabelled resources are protected and must never be deleted by this procedure.
