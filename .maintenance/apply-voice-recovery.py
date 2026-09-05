from pathlib import Path

p = Path('src/my_data_hub/voice_intake_v2/store.py')
s = p.read_text()
s = s.replace('from .contracts import', 'from .checkpoint import CheckpointError, StageCheckpoint\nfrom .contracts import', 1)
s = s.replace('    chunks: tuple[ChunkReceipt, ...]\n', '    chunks: tuple[ChunkReceipt, ...]\n    github_verified: bool = False\n', 1)
s = s.replace('SELECT create_sha256 FROM sessions WHERE session_id=?', 'SELECT create_sha256,create_json FROM sessions WHERE session_id=?')
s = s.replace('if row["create_sha256"] != digest:', 'if row["create_sha256"] != digest and not self._same_capture(row["create_json"], payload):', 1)
s = s.replace('        digest = self._canonical(request.model_dump(mode="json"))[1]\n', '        payload = request.model_dump(mode="json")\n        digest = self._canonical(payload)[1]\n', 1)
s = s.replace('if row["create_sha256"] != digest:', 'if row["create_sha256"] != digest and not self._same_capture(row["create_json"], payload):', 1)
needle = '    def existing_session(self, request: SessionCreateRequest) -> StatusResponse | None:\n'
s = s.replace(needle, '''    @staticmethod
    def _same_capture(stored_json: str, incoming: dict[str, Any]) -> bool:
        # APK version is transport telemetry, not immutable capture identity.
        # Keep the original payload/hash; tolerate no other metadata changes.
        original = json.loads(stored_json)
        current = dict(incoming)
        original.pop("client_version", None)
        current.pop("client_version", None)
        return bool(original == current)

''' + needle)
start = s.index('                # Repeating the same authenticated complete request')
end = s.index('                return self.status(session_id, connection=connection), True', start)
s = s[:start] + '''                # A duplicated transport request is never consent to another
                # paid inference. Safe retries are scheduled by the server.
''' + s[end:]
s = s.replace("OR (state='waiting_quota' AND retry_at<=?))", "OR (state IN ('waiting_quota','retryable_error')\n                              AND retryable=1 AND retry_at<=?))")
s = s.replace('state = "normalizing" if row["transcript_json"] is None else (\n                "summarizing" if row["summary_json"] is None else "publishing"\n            )', 'state = "publishing" if row["summary_json"] is not None else "normalizing"')
s = s.replace('                chunks=chunks,\n', '                chunks=chunks, github_verified=bool(row["github_verified"]),\n')
start = s.index('    def fence_ambiguous_inference(self) -> int:')
end = s.index('    def _owned_update', start)
s = s[:start] + '''    def fence_ambiguous_inference(self) -> int:
        """Recover complete receipts; fence only an unreceipted send boundary."""
        now = self._clock()
        fenced = 0
        with self._transaction() as connection:
            rows = connection.execute(
                """SELECT * FROM sessions WHERE state IN ('transcribing','summarizing')
                   AND lease_until<?""", (now,),
            ).fetchall()
            for row in rows:
                stage = "transcript" if row["state"] == "transcribing" else "summary"
                checkpoint = StageCheckpoint(
                    self.session_directory(row["session_id"]), row["session_id"],
                    stage, row["complete_sha256"],
                )
                try:
                    saved = checkpoint.load()
                except CheckpointError:
                    saved = None
                if saved is not None:
                    state, error, reconcile = "queued", None, 0
                else:
                    state, error, reconcile = "reconciliation_required", "provider_outcome_ambiguous", 1
                    fenced += 1
                connection.execute(
                    """UPDATE sessions SET state=?,retryable=0,error_code=?,
                       reconciliation_required=?,lease_owner=NULL,lease_until=NULL,updated_at=?
                       WHERE session_id=?""",
                    (state, error, reconcile, now, row["session_id"]),
                )
        return fenced

    def renew_lease(self, session_id: str, owner: str, lease_seconds: int) -> None:
        self._owned_update(session_id, owner, "lease_until=?", (self._clock() + lease_seconds,))

''' + s[end:]
s = s.replace('WHERE session_id=? AND lease_owner=?",\n                (*values, now, session_id, owner),', 'WHERE session_id=? AND lease_owner=? AND lease_until>?",\n                (*values, now, session_id, owner, now),')
s = s.replace("transcript_limiter_json=?,state='summarizing'", "transcript_limiter_json=?,state='normalizing'")
s = s.replace('"waiting_quota" if retryable and retry_at is not None else "retryable_error"', '"waiting_quota" if retryable and retry_at is not None and (\n                "quota" in code or "rate_limit" in code or "429" in code\n            ) else "retryable_error"')
p.write_text(s)

p = Path('src/my_data_hub/voice_intake_v2/inference.py')
s = p.read_text()
s = s.replace('from collections.abc import Callable, Mapping, Sequence', 'from collections.abc import Callable, Mapping, Sequence\nfrom dataclasses import asdict')
s = s.replace('from .contracts import InferenceReceipt', 'from .checkpoint import AccountingPending, StageCheckpoint\nfrom .contracts import InferenceReceipt')
s = s.replace('self, *, audio_path: Path, recorded_audio_ms: int, terminology: dict[str, Any]\n', 'self, *, audio_path: Path, recorded_audio_ms: int, terminology: dict[str, Any],\n        checkpoint: StageCheckpoint | None = None,\n')
s = s.replace('self, *, transcript: dict[str, Any], terminology: dict[str, Any]\n', 'self, *, transcript: dict[str, Any], terminology: dict[str, Any],\n        checkpoint: StageCheckpoint | None = None,\n')
s = s.replace('schema_name=TRANSCRIPT_SCHEMA_NAME,', 'schema_name=TRANSCRIPT_SCHEMA_NAME, checkpoint=checkpoint,', 1)
s = s.replace('preflight=preflight, schema_name=SUMMARY_SCHEMA_NAME,', 'preflight=preflight, schema_name=SUMMARY_SCHEMA_NAME, checkpoint=checkpoint,', 1)
s = s.replace('        preflight: LimiterPreflight | None = None,\n', '        preflight: LimiterPreflight | None = None,\n        checkpoint: StageCheckpoint | None = None,\n', 1)
s = s.replace('        try:\n            response = await self.requester.request_json(', '''        try:
            if checkpoint is not None:
                checkpoint.dispatch()
            response = await self.requester.request_json(''', 1)
old = '''        await self._finalize(lease, started, usage, "succeeded", None)
        public = self.limiter.public_lease(lease, actual_tpm=usage.total_tokens if usage else None)
        return InferenceReceipt(value=value.model_dump(mode="json"), request_uid=request_uid, limiter=public)
'''
new = '''        public = self.limiter.public_lease(lease, actual_tpm=usage.total_tokens if usage else None)
        receipt = InferenceReceipt(value=value.model_dump(mode="json"), request_uid=request_uid, limiter=public)
        if checkpoint is None:
            await self._finalize(lease, started, usage, "succeeded", None)
            return receipt
        checkpoint.save(receipt, {
            "lease": asdict(lease), "usage": usage.model_dump(mode="json") if usage else None,
            "duration_ms": int((self.clock() - started) * 1000),
        })
        # The response and request identity survive even if the limiter is down.
        return await self.resume_receipt(checkpoint)

    async def resume_receipt(self, checkpoint: StageCheckpoint) -> InferenceReceipt:
        saved = checkpoint.load()
        if saved is None:
            raise ValueError("missing inference checkpoint")
        receipt, accounting = saved
        if accounting is not None:
            lease = LimiterLease(**accounting["lease"])
            usage = ModelUsage.model_validate(accounting["usage"]) if accounting["usage"] else None
            try:
                await self.limiter.finalize_generate_content(
                    lease, usage=usage, duration_ms=accounting["duration_ms"],
                    provider_status="succeeded", error_type=None, error_code=None, error_message=None,
                )
            except Exception as exc:
                raise AccountingPending("receipt_accounting_pending") from exc
            checkpoint.save(receipt)
        return receipt
'''
assert old in s
s = s.replace(old, new)
p.write_text(s)

p = Path('src/my_data_hub/voice_intake_v2/worker.py')
s = p.read_text()
s = s.replace('import os\n', 'import shutil\n')
s = s.replace('from .contracts import', 'from .checkpoint import (\n    AccountingPending, CheckpointError, StageCheckpoint, atomic_json, fingerprint,\n)\nfrom .contracts import', 1)
start = s.index('def _atomic_json')
end = s.index('class VoiceIntakeV2Worker', start)
s = s[:start] + s[end:]
s = s.replace('self, *, audio_path: Path, recorded_audio_ms: int, terminology: dict[str, Any]\n', 'self, *, audio_path: Path, recorded_audio_ms: int, terminology: dict[str, Any],\n        checkpoint: StageCheckpoint | None = None,\n')
s = s.replace('self, *, transcript: dict[str, Any], terminology: dict[str, Any]\n', 'self, *, transcript: dict[str, Any], terminology: dict[str, Any],\n        checkpoint: StageCheckpoint | None = None,\n')
s = s.replace('            with suppress(Exception):\n                await self.process_once()', '''            try:
                await self.process_once()
            except Exception as exc:
                # Never log exception messages or tracebacks containing dictated text.
                LOGGER.error("voice_v2_worker_failure type=%s", type(exc).__name__)''')
s = s.replace('        try:\n            await self._process(session)', '''        heartbeat = asyncio.create_task(self._heartbeat(session.session_id))
        try:
            await self._process(session)''', 1)
s = s.replace('self._clock() + exc.retry_after_seconds\n                if not exc.sent and exc.retryable and exc.retry_after_seconds else None', 'self._clock() + (exc.retry_after_seconds or 30)\n                if not exc.sent and exc.retryable and not exc.ambiguous else None')
s = s.replace('retryable=exc.retryable and not ambiguous, retry_at=retry_at,', 'retryable=exc.retryable and not ambiguous and not exc.sent, retry_at=retry_at,')
s = s.replace('self._clock() + exc.retry_after_seconds\n                if exc.retryable and exc.retry_after_seconds\n                else None', 'self._clock() + (exc.retry_after_seconds or 30)\n                if exc.retryable and not exc.reconciliation_required else None')
s = s.replace('        except StoreError as exc:\n', '''        except AccountingPending:
            self.store.mark_error(
                session.session_id, self.owner, code="receipt_accounting_pending",
                retryable=True, retry_at=self._clock() + 30,
            )
        except CheckpointError:
            self.store.mark_error(
                session.session_id, self.owner, code="checkpoint_invalid", retryable=False,
                reconciliation_required=True,
            )
        except StoreError as exc:
''', 1)
s = s.replace('            retryable = exc.code == "server_audio_purge_failed"', '''            if exc.code == "worker_lease_lost":
                return True
            retryable = exc.code == "server_audio_purge_failed"''')
s = s.replace('code=exc.code, retryable=retryable,\n                reconciliation_required=False,', 'code=exc.code, retryable=retryable,\n                retry_at=self._clock() + 30 if retryable else None,\n                reconciliation_required=False,')
s = s.replace('        return True\n\n    async def _process', '''        finally:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat
        return True

    async def _heartbeat(self, session_id: str) -> None:
        while True:
            await asyncio.sleep(self.settings.lease_seconds / 3)
            try:
                self.store.renew_lease(session_id, self.owner, self.settings.lease_seconds)
            except Exception as exc:
                LOGGER.error("voice_v2_lease_renewal_failure session_id=%s type=%s",
                             session_id, type(exc).__name__)
                return

    def _checkpoint(self, session: ClaimedSession, stage: str) -> StageCheckpoint:
        return StageCheckpoint(
            self.store.session_directory(session.session_id), session.session_id,
            stage, fingerprint(session.complete),
            lambda: self.store.set_state(session.session_id, self.owner,
                                        "transcribing" if stage == "transcript" else "summarizing"),
        )

    async def _restore(self, checkpoint: StageCheckpoint) -> InferenceReceipt | None:
        saved = checkpoint.load()
        if saved is None:
            return None
        receipt, accounting = saved
        if accounting is not None:
            resume = getattr(self.inference, "resume_receipt", None)
            if resume is None:
                raise CheckpointError("accounting_recovery_unavailable")
            await resume(checkpoint)
        return receipt

    def _require_capacity(self) -> None:
        if shutil.disk_usage(self.store.root).free < 64 * 1024 * 1024 + 4 * self.settings.max_json_bytes:
            raise StageFailure("spool_capacity_low", sent=False, retryable=True, retry_after_seconds=60)

    async def _process''')
s = s.replace('        normalized = directory / "normalized" / "session.mp3"', '''        if session.github_verified:
            self.store.renew_lease(session.session_id, self.owner, self.settings.lease_seconds)
            self.store.purge_audio(session.session_id)
            self.store.finish_purge(session.session_id, self.owner)
            return
        normalized = directory / "normalized" / "session.mp3"''')
start = s.index('        if transcript is None:\n')
end = s.index('        projection = ', start)
old = s[start:end]
chunkstart = old.index('            paths: list[Path] = []')
chunkend = old.index('            self.store.set_state(')
chunkblock = old[chunkstart:chunkend]
new = '''        if transcript is None:
            checkpoint = self._checkpoint(session, "transcript")
            receipt = await self._restore(checkpoint)
            if receipt is None:
                self._require_capacity()
''' + ''.join('    ' + line + '\n' for line in chunkblock.splitlines()) + '''                receipt = await self.inference.transcribe(
                    audio_path=normalized, recorded_audio_ms=session.complete["recorded_audio_ms"],
                    terminology=session.terminology, checkpoint=checkpoint,
                )
                checkpoint.save(receipt)
            atomic_json(directory / "transcript.json", receipt.value)
            self.store.persist_transcript(
                session.session_id, self.owner, receipt.value, receipt.request_uid, receipt.limiter
            )
            transcript = receipt.value
        if summary is None:
            checkpoint = self._checkpoint(session, "summary")
            receipt = await self._restore(checkpoint)
            if receipt is None:
                self._require_capacity()
                receipt = await self.inference.summarize(
                    transcript=transcript, terminology=session.terminology, checkpoint=checkpoint,
                )
                checkpoint.save(receipt)
            atomic_json(directory / "summary.json", receipt.value)
            self.store.persist_summary(
                session.session_id, self.owner, receipt.value, receipt.request_uid, receipt.limiter
            )
            summary = receipt.value
'''
s = s[:start] + new + s[end:]
p.write_text(s)
p = Path('src/my_data_hub/voice_intake_v2/api.py')
s = p.read_text().replace('retryable: bool = False,', 'retryable: bool | None = None,', 1)
s = s.replace('"retryable": retryable,', '"retryable": status == 503 if retryable is None else retryable,', 1)
p.write_text(s)

p = Path('tests/voice_intake_v2/test_worker.py')
s = p.read_text()
s = s.replace('async def test_sent_truncation_waits_for_explicit_resume_and_logs_only_sanitized_diagnostics(', 'async def test_sent_truncation_does_not_treat_duplicate_complete_as_consent_and_logs_safely(')
s = s.replace('assert failed.retryable and failed.retry_at is None', 'assert not failed.retryable and failed.retry_at is None', 1)
s = s.replace('''    assert await worker.process_once()
    assert store.status(SESSION_ID).state == "published_verified"
    assert [call[0] for call in inference.calls] == ["transcribe", "transcribe", "summarize"]''', '''    assert not await worker.process_once()
    assert [call[0] for call in inference.calls] == ["transcribe"]''', 1)
for testname in ['test_github_retry_reuses_durable_inference_without_new_provider_calls', 'test_summary_retry_reuses_durable_transcript_without_new_transcription', 'test_failed_audio_deletion_never_marks_server_audio_purged']:
    start = s.index('async def ' + testname)
    end = s.find('\n\n@pytest.mark', start)
    if end < 0:
        end = len(s)
    chunk = s[start:end]
    chunk = chunk.replace('    store.complete(SESSION_ID, matching_complete(store, complete_request))', '''    # No new request from the phone: the server owns the retry deadline.
    from datetime import datetime
    deadline = datetime.fromisoformat(store.status(SESSION_ID).retry_at).timestamp() + 1
    store._clock = lambda: deadline
    worker._clock = lambda: deadline''')
    s = s[:start] + chunk + s[end:]
p.write_text(s)
