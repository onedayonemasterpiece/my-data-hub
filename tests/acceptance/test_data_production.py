from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from my_data_hub.acceptance.data_production import (
    DELETE_PROJECT_SQL,
    INSERT_PROJECT_SQL,
    AtomicJsonStateStore,
    ControlPlaneDataWorkloadGateway,
    ProductionCapabilityBlocker,
    ProductionDataWorkloadConfig,
    ProductionDataWorkloadReceipt,
    load_owner_authorization,
)
from my_data_hub.acceptance.data_workloads import (
    BloggerTerminalEvidence,
    CheckpointEvidence,
    DataWorkloadPlan,
    DataWorkloadState,
    FixedChangeIntent,
)
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.workloads.bloggers.importer import batch_identity
from my_data_hub.workloads.bloggers.master_stage import (
    BLOGGER_REPLAY_STAGE_SCHEMA,
    BloggerDuplicateDecision,
    BloggerDuplicateResolutionEnvelope,
    BloggerDuplicateReviewGroup,
    BloggerDuplicateReviewInputs,
    BloggerDuplicateReviewMember,
    BloggerMigrationRequest,
    BloggerQuarantineReceipt,
    resolution_matches_quarantine,
)

H = "a" * 64
H2 = "b" * 64
H3 = "c" * 64


def uid(n: int) -> UUID:
    return UUID(int=n)


@pytest.fixture
def plan() -> DataWorkloadPlan:
    query = "Калининград"
    return DataWorkloadPlan(
        matrix_id=uid(1),
        source_commit="e" * 40,
        blogger_project_id=uid(2),
        blogger_snapshot_at=datetime(2026, 8, 9, tzinfo=UTC),
        blogger_source_revision="d" * 40,
        embedding_probe_query_sha256=hashlib.sha256(query.encode()).hexdigest(),
    )


@pytest.fixture
def config() -> ProductionDataWorkloadConfig:
    return ProductionDataWorkloadConfig(
        control_base_url="https://control.example",
        mcp_endpoint="https://mcp.example/mcp",
        blogger_v1_operation_id=uid(10),
        blogger_v2_operation_id=uid(11),
        probe_query="Калининград",
        timeout_seconds=60,
        poll_seconds=1,
    )


class Control:
    def __init__(self) -> None:
        self.posts: list[tuple[str, dict[str, object]]] = []
        self.status: dict[str, object] = {}

    async def get(self, path: str):
        return self.status

    async def post(self, path: str, payload):
        exact = dict(payload)
        self.posts.append((path, exact))
        if "blogger-closure" in path:
            request = BloggerMigrationRequest.model_validate(exact)
            return {
                "request_id": str(request.request_id),
                "request_sha256": request.request_sha256,
                "state": "REQUESTED",
                "created": True,
            }
        return {
            "request_id": exact["request_id"],
            "request_sha256": hashlib.sha256(canonical_json_bytes(exact)).hexdigest(),
            "state": "REQUESTED",
            "created": True,
        }


class Mcp:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, object]]] = []
        self.preview_action = "insert"

    async def call(self, profile: str, tool: str, arguments):
        exact = dict(arguments)
        self.calls.append((profile, tool, exact))
        if tool == "master.rotation.request":
            return {
                "accepted": True,
                "execution_supported": True,
                "duplicate": False,
                "request_sha256": H2,
                "operation_id": "server-operation-17",
                "state": "REQUESTED",
            }
        if tool == "data.change.preview":
            self.preview_action = "insert" if exact["sql"] == INSERT_PROJECT_SQL else "delete"
            return {
                "operation_id": H if self.preview_action == "insert" else H2,
                "affected_rows": 1,
                "preview_receipt": f"signed-{self.preview_action}-receipt",
                "pre_change_checkpoint_id": str(uid(100)),
            }
        if tool == "data.change.apply":
            return {
                "operation_id": H if self.preview_action == "insert" else H2,
                "affected_rows": 1,
                "canonical_revision": int(exact["expected_revision"]) + 1,
            }
        raise AssertionError(tool)


def gateway(plan: DataWorkloadPlan, config: ProductionDataWorkloadConfig, control=None, mcp=None):
    return ControlPlaneDataWorkloadGateway(
        plan=plan,
        config=config,
        control=control or Control(),
        mcp=mcp or Mcp(),
    )


@pytest.mark.asyncio
async def test_h5_persists_server_request_hash_not_core_intent(plan, config):
    control = Control()
    value = await gateway(plan, config, control=control).start_blogger_v1(
        request_id=plan.identity("fm16:v1"), intent_sha256=H, plan=plan
    )
    request = BloggerMigrationRequest.model_validate(control.posts[0][1])
    assert value.request_sha256 == request.request_sha256
    assert value.request_sha256 != H
    assert value.operation_id == str(request.request_id)


@pytest.mark.asyncio
async def test_missing_h5_quarantine_projection_is_precise_fail_closed_blocker(plan, config):
    control = Control()
    request_id = plan.identity("fm16:v1")
    control.status = {
        "request_id": str(request_id),
        "request_sha256": H,
        "state": "FAILED",
        "failure_code": "BloggerMigrationQuarantined",
    }
    with pytest.raises(ProductionCapabilityBlocker) as caught:
        await gateway(plan, config, control=control).observe_blogger(request_id)
    assert caught.value.code == "FM16_H5_QUARANTINE_PROJECTION_UNAVAILABLE"
    assert control.posts == []


@pytest.mark.asyncio
async def test_real_h5_quarantine_status_drives_exact_owner_bound_v2_replay(
    plan: DataWorkloadPlan, config: ProductionDataWorkloadConfig, tmp_path: Path
) -> None:
    """Exercise the integrated H5 public projection, not a hand-written substitute."""

    control = Control()
    source = BloggerMigrationRequest(
        request_id=plan.identity("fm16:v1"),
        operation_id=config.blogger_v1_operation_id,
        project_id=plan.blogger_project_id,
        snapshot_at=plan.blogger_snapshot_at,
        source_revision=plan.blogger_source_revision,
    )
    canonical_actor = uid(40)
    other_actor = uid(41)
    receipt = BloggerQuarantineReceipt(
        request_id=source.request_id,
        operation_id=source.operation_id,
        request_sha256=source.request_sha256,
        master_instance_id=uid(42),
        run_id="owner/postgres-master/run-1",
        attempt_id="attempt-1",
        epoch=7,
        export_batch_id=batch_identity(plan.blogger_snapshot_at, source.expected_rows),
        row_count=266,
        raw_count=266,
        dispositioned_count=266,
        undispositioned_count=0,
        quarantined_count=2,
        logical_sha256="1" * 64,
        record_id_set_sha256="2" * 64,
        canonical_outcome_sha256="3" * 64,
        duplicate_group_count=1,
        duplicate_groups_pending=1,
        duplicate_review_inputs=BloggerDuplicateReviewInputs(
            groups=(
                BloggerDuplicateReviewGroup(
                    identity_sha256="4" * 64,
                    members=(
                        BloggerDuplicateReviewMember(
                            record_id="blogger-001", projected_actor_id=canonical_actor
                        ),
                        BloggerDuplicateReviewMember(
                            record_id="blogger-002", projected_actor_id=other_actor
                        ),
                    ),
                ),
            )
        ),
    )
    # This is the exact shape composed by H5 blogger_closure_status from the
    # receipt's three public projections. The internal receipt itself is absent.
    control.status = {
        "request_id": str(source.request_id),
        "operation_id": str(source.operation_id),
        "request_sha256": source.request_sha256,
        "state": "FAILED",
        "failure_code": receipt.failure_code,
        "quarantine_receipt_sha256": receipt.receipt_sha256,
        "quarantine_evidence": receipt.quarantine_evidence,
        "duplicate_review": receipt.duplicate_review,
        "duplicate_review_inputs": receipt.duplicate_review_inputs.model_dump(mode="json"),
    }
    observed_gateway = gateway(plan, config, control=control)
    observed = await observed_gateway.observe_blogger(source.request_id)
    assert observed.state == "FAILED"
    assert observed.quarantine is not None
    assert observed.quarantine.request_sha256 == source.request_sha256
    review = await observed_gateway.duplicate_review(source.request_id)

    group = receipt.duplicate_review_inputs.groups[0]
    authorizer = "owner-review:fm16"
    envelope = BloggerDuplicateResolutionEnvelope(
        authorization_id=uid(43),
        authorized_by=authorizer,
        authorized_at=datetime(2026, 8, 11, tzinfo=UTC),
        source_request_id=source.request_id,
        source_operation_id=source.operation_id,
        source_request_sha256=source.request_sha256,
        export_batch_id=receipt.export_batch_id,
        project_id=plan.blogger_project_id,
        snapshot_at=plan.blogger_snapshot_at,
        source_revision=plan.blogger_source_revision,
        decisions=(
            BloggerDuplicateDecision(
                identity_sha256=group.identity_sha256,
                canonical_record_id="blogger-001",
                canonical_actor_id=canonical_actor,
                member_record_ids=tuple(member.record_id for member in group.members),
                decided_by=authorizer,
                reason="Owner reviewed the exact H5 duplicate projection.",
            ),
        ),
    )
    assert resolution_matches_quarantine(envelope, receipt)
    envelope_path = tmp_path / "owner-envelope.json"
    envelope_path.write_bytes(canonical_json_bytes(envelope.model_dump(mode="json")))
    envelope_path.chmod(0o600)
    loaded, authorization = load_owner_authorization(envelope_path)
    assert loaded == envelope
    assert authorization.binds(review)

    replay_gateway = ControlPlaneDataWorkloadGateway(
        plan=plan,
        config=config,
        control=control,
        mcp=Mcp(),
        owner_envelope=loaded,
    )
    replay_id = plan.identity("fm16:v2")
    accepted = await replay_gateway.start_blogger_v2(
        request_id=replay_id,
        intent_sha256=H3,
        authorization=authorization,
    )
    replay_request = BloggerMigrationRequest.model_validate(control.posts[-1][1])
    assert replay_request.schema_version == BLOGGER_REPLAY_STAGE_SCHEMA
    assert replay_request.duplicate_resolution == envelope
    assert accepted.request_sha256 == replay_request.request_sha256
    assert accepted.state == "REQUESTED"
    assert accepted.outcome == "accepted"

    # Mirror the H5 atomic-CAS status returned immediately after insertion and
    # prove observation retains the exact server request hash/state.
    control.status = {
        "request_id": str(replay_id),
        "request_sha256": accepted.request_sha256,
        "state": accepted.state,
    }
    replay_status = await replay_gateway.observe_blogger(replay_id)
    assert replay_status.request_sha256 == replay_request.request_sha256
    assert replay_status.state == "REQUESTED"


@pytest.mark.asyncio
async def test_fm17_keeps_server_assigned_rotation_identity(plan, config):
    mcp = Mcp()
    checkpoint = CheckpointEvidence(
        checkpoint_id=uid(90),
        generation=3,
        exact_version_ref="owner/checkpoints/3",
        manifest_sha256=H,
        canonical_revision=10,
    )
    value = await gateway(plan, config, mcp=mcp).start_restore(
        idempotency_key_sha256=H3, checkpoint=checkpoint, expected_epoch=7
    )
    assert value.operation_id == "server-operation-17"
    assert value.request_sha256 == H2
    assert mcp.calls == [
        (
            "operator",
            "master.rotation.request",
            {
                "checkpoint_id": str(uid(90)),
                "exact_version_ref": "owner/checkpoints/3",
                "expected_active_epoch": 7,
                "expected_canonical_revision": 10,
                "timeout_seconds": 60,
                "idempotency_key": f"h6-fm17:{H3}",
            },
        )
    ]


@pytest.mark.asyncio
async def test_fm18_fm19_share_one_exact_two_model_request(plan, config):
    control = Control()
    value = gateway(plan, config, control=control)
    checkpoint = CheckpointEvidence(
        checkpoint_id=uid(90),
        generation=3,
        exact_version_ref="owner/checkpoints/3",
        manifest_sha256=H,
        canonical_revision=10,
    )
    blogger = BloggerTerminalEvidence(
        request_id=uid(80),
        request_sha256=H,
        receipt_sha256=H2,
        operation_id=uid(81),
        import_schema="region-talk-ydb-bloggers-import-receipt.v3",
        export_batch_id=uid(82),
        dispositions={"normalized": 264, "deduplicated": 2},
        duplicate_group_count=1,
        actor_count=265,
        account_count=266,
        logical_sha256=H,
        record_id_set_sha256=H2,
        canonical_outcome_sha256=H3,
        source_master_instance_id=uid(83),
        source_run_id=uid(84),
        source_epoch=7,
        canonical_revision=10,
        checkpoint=checkpoint,
    )
    accepted = await value.start_embedding(
        request_id=plan.identity("fm18-19:embedding"),
        intent_sha256=H3,
        blogger=blogger,
        probe_query_sha256=plan.embedding_probe_query_sha256,
    )
    assert accepted.outcome == "accepted"
    assert len(control.posts) == 1
    payload = control.posts[0][1]
    assert payload["blogger_receipt_id"] == str(blogger.request_id)
    assert payload["blogger_receipt_sha256"] == blogger.receipt_sha256
    assert len(payload["worker_assets"]) == 2


@pytest.mark.asyncio
async def test_fm21_uses_only_fixed_project_sql_and_replays_preview_receipt(plan, config):
    mcp = Mcp()
    value = gateway(plan, config, mcp=mcp)
    intent = FixedChangeIntent(
        action="insert",
        fixture_project_id=plan.identity("fm21:project"),
        fixture_sha256=hashlib.sha256(
            canonical_json_bytes(
                {
                    "contract": "fm21_hub_project_fixture.v1",
                    "matrix_id": str(plan.matrix_id),
                    "project_id": str(plan.identity("fm21:project")),
                }
            )
        ).hexdigest(),
        expected_revision=11,
        idempotency_key_sha256=plan.key_sha256("fm21:insert"),
    )
    preview = await value.preview_fixed_change(intent)
    assert preview.affected_rows == 1
    assert mcp.calls[0][2]["sql"] == INSERT_PROJECT_SQL
    applied = await value.apply_fixed_change(preview)
    assert applied.outcome == "accepted" and applied.committed_revision == 12
    assert mcp.calls[1][2]["sql"] == INSERT_PROJECT_SQL
    assert mcp.calls[1][2]["preview_receipt"] == "signed-insert-receipt"
    assert DELETE_PROJECT_SQL == "DELETE FROM hub.project WHERE project_id=$1"


def test_owner_envelope_requires_mode_0600_and_derives_only_hashes(tmp_path: Path):
    source = Path("examples/bloggers/region-talk-blogger-duplicate-resolution-envelope.v1.example.json")
    target = tmp_path / "owner-envelope.json"
    target.write_bytes(source.read_bytes())
    target.chmod(0o644)
    with pytest.raises(ProductionCapabilityBlocker) as caught:
        load_owner_authorization(target)
    assert caught.value.code == "FM16_OWNER_ENVELOPE_PERMISSIONS_INVALID"
    target.chmod(0o600)
    envelope, authorization = load_owner_authorization(target)
    assert authorization.envelope_sha256 == envelope.envelope_sha256
    serialized = authorization.model_dump_json()
    assert envelope.authorized_by not in serialized
    assert envelope.decisions[0].canonical_record_id not in serialized


def test_atomic_state_store_is_mode_0600_and_exactly_resumable(tmp_path: Path, plan):
    store = AtomicJsonStateStore(tmp_path / "state.json")
    state = DataWorkloadState.initial(plan)
    store.persist(state)
    assert (store.path.stat().st_mode & 0o777) == 0o600
    assert store.load(plan) == state


def test_production_examples_validate_against_fixed_models():
    root = Path(__file__).parents[2]
    plan_value = json.loads((root / "examples/provider/operational-data-workload-plan.v1.example.json").read_text())
    config_value = json.loads((root / "examples/provider/data-workload-production-config.v1.example.json").read_text())
    receipt_value = json.loads(
        (root / "examples/provider/data-workload-production-receipt.v1.example.json").read_text()
    )
    exact_plan = DataWorkloadPlan.model_validate(plan_value)
    exact_config = ProductionDataWorkloadConfig.model_validate(config_value)
    ProductionDataWorkloadReceipt.model_validate(receipt_value)
    ControlPlaneDataWorkloadGateway(
        plan=exact_plan,
        config=exact_config,
        control=Control(),
        mcp=Mcp(),
    )
