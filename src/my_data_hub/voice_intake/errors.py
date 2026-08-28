from __future__ import annotations


class VoiceIntakeError(RuntimeError):
    def __init__(
        self,
        code: str,
        *,
        retryable: bool = False,
        retry_after_seconds: int | None = None,
        status_code: int = 502,
        reconciliation_required: bool = False,
    ) -> None:
        self.code = code
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds
        self.status_code = status_code
        self.reconciliation_required = reconciliation_required
        super().__init__(code)


class GitHubPublicationConflict(VoiceIntakeError):
    def __init__(self, code: str = "github_publication_conflict") -> None:
        super().__init__(code, retryable=True, status_code=409)
