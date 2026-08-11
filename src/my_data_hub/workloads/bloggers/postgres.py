"""Transactional master-side writer for the bounded blogger snapshot.

The caller owns one PostgreSQL transaction covering the full verified snapshot.
No source row or credential is written to the devstand control ledger.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid5

from psycopg.types.json import Jsonb

from .schema import SOURCE_DATABASE_PATH, SOURCE_QUERY_SHA256, SOURCE_TABLE, BloggerSourceRow
from .transform import BloggerDisposition, BloggerProjection

_OBJECT_NAMESPACE = UUID("1e783e11-4872-58ef-a123-096f40890d51")


class BloggerReplayConflict(RuntimeError):
    """The same immutable source identity was reused with different bytes."""


@dataclass(frozen=True, slots=True)
class WriteOutcome:
    record_id: str
    actor_id: UUID
    disposition: BloggerDisposition
    replayed: bool
    duplicate_group_ids: tuple[UUID, ...]


def _id(kind: str, value: str) -> UUID:
    return uuid5(_OBJECT_NAMESPACE, f"{kind}:{value}")


def _identity_hash(platform: str, normalized_url: str) -> str:
    return hashlib.sha256(f"{platform}\0{normalized_url}".encode()).hexdigest()


class PostgresBloggerWriter:
    """Write exactly one row through landing, canonical, provenance and disposition."""

    mapping_version = "region-talk-bloggers.v1"

    def write_row(
        self,
        cursor: Any,
        *,
        export_batch_id: UUID,
        project_id: UUID,
        row: BloggerSourceRow,
        projection: BloggerProjection,
    ) -> WriteOutcome:
        if row.record_id != projection.record_id:
            raise ValueError("source/projection identity mismatch")
        raw_record_id = _id("raw", f"{export_batch_id}:{SOURCE_TABLE}:{row.record_id}")
        existing = cursor.execute(
            "SELECT payload_sha256 FROM migration.raw_record WHERE raw_record_id=%s",
            (raw_record_id,),
        ).fetchone()
        if existing is not None:
            if existing[0] != row.payload_sha256:
                raise BloggerReplayConflict("same raw_record_id has different payload hash")
            disposition = cursor.execute(
                "SELECT disposition FROM migration.row_disposition WHERE raw_record_id=%s",
                (raw_record_id,),
            ).fetchone()
            if disposition is None:
                raise BloggerReplayConflict("replayed raw record lacks terminal disposition")
            return WriteOutcome(
                row.record_id,
                projection.actor_id,
                BloggerDisposition(disposition[0]),
                True,
                (),
            )

        cursor.execute(
            """
            INSERT INTO migration.raw_record(
                raw_record_id,export_batch_id,source_table,source_pk,row_kind,
                source_updated_at,payload,payload_sha256
            ) VALUES (%s,%s,%s,%s,'region_talk_external_blogger_evidence',%s,%s,%s)
            """,
            (
                raw_record_id,
                export_batch_id,
                SOURCE_TABLE,
                row.record_id,
                row.updated_at,
                Jsonb(row.payload()),
                row.payload_sha256,
            ),
        )
        cursor.execute(
            """
            INSERT INTO hub.actor(actor_id,actor_type,display_name,canonical_name,summary,metadata)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (actor_id) DO NOTHING
            """,
            (
                projection.actor_id,
                projection.actor_kind,
                projection.display_name,
                projection.display_name.casefold(),
                projection.summary,
                Jsonb({"blogger_contract": "bloggers_ru_v1"}),
            ),
        )
        provenance_id = _id("provenance", f"{export_batch_id}:{row.record_id}")
        cursor.execute(
            """
            INSERT INTO hub.provenance_event(
                provenance_event_id,project_id,subject_type,subject_id,event_type,
                actor_kind,actor_ref,query_text,source_uri,observed_at,evidence
            ) VALUES (%s,%s,'actor',%s,'ydb_blogger_import','system',%s,%s,%s,%s,%s)
            ON CONFLICT (provenance_event_id) DO NOTHING
            """,
            (
                provenance_id,
                project_id,
                projection.actor_id,
                self.mapping_version,
                f"sha256:{SOURCE_QUERY_SHA256}",
                f"ydb://{SOURCE_DATABASE_PATH}/{SOURCE_TABLE}/{row.record_id}",
                row.updated_at,
                Jsonb({"export_batch_id": str(export_batch_id), "payload_sha256": row.payload_sha256}),
            ),
        )
        cursor.execute(
            """
            INSERT INTO hub.project_actor(
                project_id,actor_id,membership_kind,status,provenance_event_id,metadata
            ) VALUES (%s,%s,'blogger','included',%s,%s)
            ON CONFLICT (project_id,actor_id,membership_kind) DO NOTHING
            """,
            (project_id, projection.actor_id, provenance_id, Jsonb({"legacy_record_id": row.record_id})),
        )

        duplicate_groups: list[UUID] = []
        disposition = projection.disposition
        reason = projection.reason_code
        for account in projection.accounts:
            conflict = cursor.execute(
                "SELECT actor_id,account_id FROM hub.external_account "
                "WHERE platform=%s AND normalized_url=%s",
                (account.platform, account.normalized_url),
            ).fetchone()
            if conflict is not None and conflict[0] != projection.actor_id:
                identity_hash = _identity_hash(account.platform, account.normalized_url)
                group_id = _id("duplicate", f"{export_batch_id}:{identity_hash}")
                duplicate_groups.append(group_id)
                cursor.execute(
                    """
                    INSERT INTO migration.duplicate_group(
                        duplicate_group_id,export_batch_id,identity_kind,identity_hash,
                        decision_status,reason
                    ) VALUES (%s,%s,'account_url',%s,'pending','same public account claimed by multiple source rows')
                    ON CONFLICT (duplicate_group_id) DO NOTHING
                    """,
                    (group_id, export_batch_id, identity_hash),
                )
                cursor.execute(
                    """
                    INSERT INTO migration.duplicate_group_member(
                        duplicate_group_id,raw_record_id,actor_id,evidence
                    ) VALUES (%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        group_id,
                        raw_record_id,
                        projection.actor_id,
                        Jsonb({"platform": account.platform, "normalized_url_sha256": identity_hash}),
                    ),
                )
                disposition = BloggerDisposition.RETAINED_RAW
                reason = "duplicate_account_pending_decision"
                continue
            cursor.execute(
                """
                INSERT INTO hub.external_account(
                    account_id,actor_id,platform,handle,url,normalized_url,status,metadata
                ) VALUES (%s,%s,%s,%s,%s,%s,'active',%s)
                ON CONFLICT (platform,normalized_url) WHERE normalized_url IS NOT NULL DO NOTHING
                """,
                (
                    _id("account", f"{projection.actor_id}:{account.platform}:{account.normalized_url}"),
                    projection.actor_id,
                    account.platform,
                    account.handle,
                    account.url,
                    account.normalized_url,
                    Jsonb({"source_record_id": row.record_id}),
                ),
            )

        cursor.execute(
            """
            INSERT INTO region_talk.blogger_profile(
                actor_id,legacy_record_id,export_batch_id,confirmation_status,
                region_relation_status,geography_signal,geography_provenance,
                source_updated_at,public_evidence_url,requires_review
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                projection.actor_id,
                row.record_id,
                export_batch_id,
                row.confirmation_status,
                row.region_relation_status,
                projection.geography_signal,
                projection.geography_provenance,
                row.updated_at,
                row.evidence_url,
                projection.requires_review or bool(duplicate_groups),
            ),
        )
        cursor.execute(
            """
            INSERT INTO migration.legacy_identity_map(
                source_system,source_table,source_pk,target_table,target_pk,
                mapping_version,mapping_kind,evidence
            ) VALUES ('ydb',%s,%s,'hub.actor',%s,%s,'created',%s)
            """,
            (
                SOURCE_TABLE,
                row.record_id,
                Jsonb({"actor_id": str(projection.actor_id)}),
                self.mapping_version,
                Jsonb({"export_batch_id": str(export_batch_id)}),
            ),
        )
        cursor.execute(
            """
            INSERT INTO migration.row_disposition(
                raw_record_id,mapping_version,disposition,target_refs,reason_code,
                transformer_sha256
            ) VALUES (%s,%s,%s,%s,%s,%s)
            """,
            (
                raw_record_id,
                self.mapping_version,
                disposition.value,
                Jsonb([{"table": "hub.actor", "id": str(projection.actor_id)}]),
                reason,
                hashlib.sha256(self.mapping_version.encode()).hexdigest(),
            ),
        )
        return WriteOutcome(
            row.record_id,
            projection.actor_id,
            disposition,
            False,
            tuple(sorted(duplicate_groups, key=str)),
        )


def canonical_outcome_hash(outcomes: list[WriteOutcome]) -> str:
    payload = [
        {
            "record_id": item.record_id,
            "actor_id": str(item.actor_id),
            "disposition": item.disposition.value,
            "duplicate_group_ids": [str(value) for value in item.duplicate_group_ids],
        }
        for item in sorted(outcomes, key=lambda item: item.record_id)
    ]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
