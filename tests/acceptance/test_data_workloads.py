from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from my_data_hub.acceptance.data_workloads import (
    BloggerAccountingEvidence,
    BloggerQuarantineEvidence,
    BloggerRequestObservation,
    BloggerTerminalEvidence,
    ChangeApplyEvidence,
    ChangePreviewEvidence,
    ChangeStatusEvidence,
    CheckpointEvidence,
    DataPhase,
    DataWorkloadEvidenceBundle,
    DataWorkloadPlan,
    DataWorkloadState,
    DataWorkloadStateMachine,
    DuplicateReviewEvidence,
    EmbeddingModelEvidence,
    EmbeddingRequestObservation,
    EmbeddingTerminalEvidence,
    MasterEvidence,
    MutationAcceptance,
    OwnerDuplicateAuthorization,
    RestoreObservation,
)
from my_data_hub.embeddings.production import WORKER_ASSETS

H = "a" * 64
H2 = "b" * 64
H3 = "c" * 64


def uid(n: int) -> UUID:
    return UUID(int=n)


def checkpoint(n: int, revision: int) -> CheckpointEvidence:
    return CheckpointEvidence(
        checkpoint_id=uid(100 + n),
        generation=n,
        exact_version_ref=f"owner/checkpoint/{n}",
        manifest_sha256=H,
        canonical_revision=revision,
    )


@pytest.fixture
def plan() -> DataWorkloadPlan:
    return DataWorkloadPlan(
        matrix_id=uid(1),
        source_commit="e" * 40,
        blogger_project_id=uid(2),
        blogger_snapshot_at=datetime(2026, 8, 11, tzinfo=UTC),
        blogger_source_revision="d" * 40,
        embedding_probe_query_sha256=H2,
    )


class Store:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.states: list[DataWorkloadState] = []

    def persist(self, state: DataWorkloadState) -> None:
        self.states.append(state)
        self.events.append(f"persist:{state.phase}")


class Gateway:
    def __init__(self, plan: DataWorkloadPlan, events: list[str], *, ambiguous_insert: bool = False) -> None:
        self.plan = plan
        self.events = events
        self.ambiguous_insert = ambiguous_insert
        self.insert_apply_calls = 0
        self.v1 = plan.identity("fm16:v1")
        self.v2 = plan.identity("fm16:v2")
        self.embedding = plan.identity("fm18-19:embedding")
        self.v1_sha = H
        self.v2_sha = H2
        self.embedding_sha = H3
        self.batch = uid(20)
        self.cp1 = checkpoint(1, 9)
        self.cp2 = checkpoint(2, 10)
        self.cp3 = checkpoint(3, 11)
        self.cp4 = checkpoint(4, 12)
        self.cp5 = checkpoint(5, 13)
        self.quarantine = BloggerQuarantineEvidence(
            request_id=self.v1,
            request_sha256=H,
            operation_id=uid(21),
            export_batch_id=self.batch,
            failure_code="BloggerMigrationQuarantined",
            quarantined_count=2,
            logical_sha256=H,
            record_id_set_sha256=H2,
            canonical_outcome_sha256=H3,
            duplicate_group_count=1,
            duplicate_groups_pending=1,
            checkpoint=self.cp1,
        )
        self.review = DuplicateReviewEvidence(
            export_batch_id=self.batch,
            source_request_id=self.v1,
            source_operation_id=uid(21),
            source_request_sha256=H,
            duplicate_group_count=1,
            duplicate_groups_pending=1,
            identity_set_sha256=H2,
            member_record_id_set_sha256=H3,
            review_projection_sha256=H,
        )
        self.terminal = BloggerTerminalEvidence(
            request_id=self.v2,
            request_sha256=H2,
            operation_id=uid(22),
            import_schema="region-talk-ydb-bloggers-import-receipt.v3",
            export_batch_id=self.batch,
            dispositions={"normalized": 264, "deduplicated": 2},
            duplicate_group_count=1,
            actor_count=265,
            account_count=266,
            logical_sha256=H,
            record_id_set_sha256=H2,
            canonical_outcome_sha256=H3,
            source_master_instance_id=uid(30),
            source_run_id=uid(31),
            source_epoch=7,
            canonical_revision=10,
            checkpoint=self.cp2,
        )
        self.accounting = BloggerAccountingEvidence(
            export_batch_id=self.batch,
            logical_sha256=H,
            record_id_set_sha256=H2,
            canonical_outcome_sha256=H3,
            actor_count=265,
            account_count=266,
            canonical_revision=10,
        )
        models = tuple(
            EmbeddingModelEvidence(
                model_exact_id=asset.model.exact_id,
                task_run_id=uid(40 + index),
                provider_ref=f"owner/worker{index}",
                provider_run_ref=f"owner/worker{index}/1",
                provider_kernel_id=index,
                source_sha256=H2,
                primary_source_sha256=asset.primary_source_sha256,
                artifact_id=uid(50 + index),
                artifact_sha256=H3,
            )
            for index, asset in enumerate(WORKER_ASSETS, 1)
        )
        self.embedding_terminal = EmbeddingTerminalEvidence(
            request_id=self.embedding,
            request_sha256=H3,
            blogger_export_batch_id=self.batch,
            blogger_canonical_revision=10,
            canonical_revision=11,
            models=models,
            checkpoint=self.cp3,
        )
        self.previews: dict[str, ChangePreviewEvidence] = {}

    def acceptance(self, operation_id: str, outcome: str = "accepted") -> MutationAcceptance:
        return MutationAcceptance(
            operation_id=operation_id,
            outcome=outcome,
            state="REQUESTED",
            response_sha256=H,
        )

    async def start_blogger_v1(self, *, request_id, intent_sha256, plan):
        self.v1_sha = intent_sha256
        self.events.append("mutate:v1")
        return self.acceptance(str(request_id))

    async def observe_blogger(self, request_id):
        if request_id == self.v1:
            quarantine = self.quarantine.model_copy(update={"request_sha256": self.v1_sha})
            return BloggerRequestObservation(
                request_id=request_id,
                request_sha256=self.v1_sha,
                state="FAILED",
                quarantine=quarantine,
            )
        terminal = self.terminal.model_copy(update={"request_sha256": self.v2_sha})
        return BloggerRequestObservation(
            request_id=request_id,
            request_sha256=self.v2_sha,
            state="CHECKPOINT_VERIFIED",
            terminal=terminal,
        )

    async def duplicate_review(self, request_id):
        self.review = self.review.model_copy(update={"source_request_sha256": self.v1_sha})
        return self.review

    async def start_blogger_v2(self, *, request_id, intent_sha256, authorization):
        self.v2_sha = intent_sha256
        self.events.append("mutate:v2")
        return self.acceptance(str(request_id))

    async def migration_accounting(self, export_batch_id):
        return self.accounting

    async def start_restore(self, *, operation_id, checkpoint, expected_epoch):
        assert expected_epoch == 7
        self.events.append("mutate:restore")
        return self.acceptance(operation_id)

    async def observe_restore(self, operation_id):
        return RestoreObservation(operation_id=operation_id, state="DURABLE_COMPLETE")

    async def active_master(self):
        return MasterEvidence(master_instance_id=uid(32), run_id=uid(33), epoch=8, canonical_revision=10)

    async def start_embedding(self, *, request_id, intent_sha256, blogger, probe_query_sha256):
        self.embedding_sha = intent_sha256
        self.events.append("mutate:embedding")
        return self.acceptance(str(request_id))

    async def observe_embedding(self, request_id):
        terminal = self.embedding_terminal.model_copy(update={"request_sha256": self.embedding_sha})
        return EmbeddingRequestObservation(
            request_id=request_id,
            request_sha256=self.embedding_sha,
            state="CHECKPOINT_VERIFIED",
            terminal=terminal,
        )

    async def preview_fixed_change(self, intent):
        if intent.action == "insert":
            affected, operation, cp = 0, H, self.cp3
        elif intent.expected_revision == 12:
            affected, operation, cp = 1, H2, self.cp4
        else:
            affected, operation, cp = 0, H3, self.cp5
        preview = ChangePreviewEvidence(
            operation_id=operation,
            request_sha256=intent.request_sha256,
            action=intent.action,
            affected_rows=affected,
            expected_revision=intent.expected_revision,
            pre_change_checkpoint_id=cp.checkpoint_id,
            preview_receipt_sha256=H,
        )
        self.previews[operation] = preview
        return preview

    async def apply_fixed_change(self, preview):
        self.events.append(f"mutate:{preview.action}")
        if preview.action == "insert":
            self.insert_apply_calls += 1
            outcome = "ambiguous" if self.ambiguous_insert and self.insert_apply_calls == 1 else "accepted"
        else:
            outcome = "accepted"
        return ChangeApplyEvidence(operation_id=preview.operation_id, outcome=outcome, response_sha256=H)

    async def fixed_change_status(self, operation_id):
        preview = self.previews[operation_id]
        post = self.cp4 if preview.action == "insert" else self.cp5
        return ChangeStatusEvidence(
            operation_id=operation_id,
            state="DURABLE_COMPLETE",
            expected_revision=preview.expected_revision,
            committed_revision=preview.expected_revision + 1,
            pre_change_checkpoint_id=preview.pre_change_checkpoint_id,
            post_change_checkpoint=post,
        )


def authorization(gateway: Gateway) -> OwnerDuplicateAuthorization:
    review = gateway.review
    return OwnerDuplicateAuthorization(
        authorization_id=uid(70),
        authorized_by_sha256=H,
        authorized_at=datetime.now(UTC),
        source_request_id=review.source_request_id,
        source_operation_id=review.source_operation_id,
        source_request_sha256=review.source_request_sha256,
        export_batch_id=review.export_batch_id,
        decision_count=review.duplicate_group_count,
        identity_set_sha256=review.identity_set_sha256,
        member_record_id_set_sha256=review.member_record_id_set_sha256,
        envelope_sha256=H3,
    )


@pytest.mark.asyncio
async def test_exact_flow_persists_identity_before_every_mutation_and_emits_no_pass(plan):
    events: list[str] = []
    store, gateway = Store(events), Gateway(plan, events)
    machine = DataWorkloadStateMachine(store)
    state = DataWorkloadState.initial(plan)
    result = None
    for _ in range(20):
        auth = authorization(gateway) if state.phase == DataPhase.AWAITING_OWNER_AUTHORIZATION else None
        result = await machine.advance(plan, state, gateway, owner_authorization=auth)
        state = result.state
        if result.outcome == "EVIDENCE_READY":
            break
    assert result is not None and result.outcome == "EVIDENCE_READY"
    assert result.evidence is not None and result.evidence.live_evidence is False
    assert [item.requirement_id for item in result.evidence.requirements] == ["FM16", "FM17", "FM18", "FM19", "FM21"]
    assert events.index("persist:FM16_V1_AMBIGUOUS") < events.index("mutate:v1")
    assert events.index("persist:FM16_V2_AMBIGUOUS") < events.index("mutate:v2")
    assert events.index("persist:FM17_RESTORE_AMBIGUOUS") < events.index("mutate:restore")
    assert events.index("persist:FM18_19_AMBIGUOUS") < events.index("mutate:embedding")
    assert events.index("persist:FM21_INSERT_AMBIGUOUS") < events.index("mutate:insert")
    assert events.index("persist:FM21_DELETE_AMBIGUOUS") < events.index("mutate:delete")


@pytest.mark.asyncio
async def test_owner_boundary_blocks_v2_and_mismatch_is_resumable(plan):
    events: list[str] = []
    store, gateway = Store(events), Gateway(plan, events)
    machine = DataWorkloadStateMachine(store)
    state = DataWorkloadState.initial(plan)
    state = (await machine.advance(plan, state, gateway)).state
    waiting = await machine.advance(plan, state, gateway)
    assert waiting.outcome == "AWAITING_OWNER_AUTHORIZATION"
    bad = authorization(gateway).model_copy(update={"identity_set_sha256": H3})
    failed = await machine.advance(plan, waiting.state, gateway, owner_authorization=bad)
    assert failed.outcome == "FAIL" and failed.resumable
    assert "mutate:v2" not in events


@pytest.mark.asyncio
async def test_ambiguous_insert_resumes_by_status_without_reapplying(plan):
    events: list[str] = []
    store, gateway = Store(events), Gateway(plan, events, ambiguous_insert=True)
    machine = DataWorkloadStateMachine(store)
    state = DataWorkloadState.initial(plan)
    for _ in range(20):
        auth = authorization(gateway) if state.phase == DataPhase.AWAITING_OWNER_AUTHORIZATION else None
        result = await machine.advance(plan, state, gateway, owner_authorization=auth)
        state = result.state
        if result.failure_code == "FM21_INSERT_AMBIGUOUS":
            break
    assert result.outcome == "FAIL" and result.resumable
    resumed = await machine.advance(plan, state, gateway)
    assert resumed.state.phase == DataPhase.FM21_INSERT_COMPLETE
    assert gateway.insert_apply_calls == 1


def test_contract_cannot_claim_live_pass(plan):
    with pytest.raises(ValidationError):
        DataWorkloadEvidenceBundle(
            matrix_id=plan.matrix_id,
            source_commit=plan.source_commit,
            outcome="PASS",
            live_evidence=True,
            requirements=(),
        )


def test_committed_examples_match_models_and_schemas():
    root = Path(__file__).parents[2]
    evidence = json.loads((root / "examples/provider/operational-data-evidence.v1.example.json").read_text())
    state = json.loads((root / "examples/provider/operational-data-workload-state.v1.example.json").read_text())
    DataWorkloadEvidenceBundle.model_validate(evidence)
    DataWorkloadState.model_validate(state)
    for name in ("operational-data-evidence.v1", "operational-data-workload-state.v1"):
        schema = json.loads((root / f"schemas/provider/{name}.schema.json").read_text())
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
