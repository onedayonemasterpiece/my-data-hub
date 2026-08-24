from __future__ import annotations

from scripts.scan_tracked_secrets import findings


def test_secret_scan_detects_strong_tokens_and_private_keys() -> None:
    github_token = "gh" + "p_" + "A" * 40
    private_key = "-----BEGIN " + "PRIVATE KEY-----"

    assert "github-token" in findings(github_token)
    assert "private-key" in findings(private_key)


def test_secret_scan_allows_only_the_exact_invalid_historical_test_pem() -> None:
    sentinel = "-----BEGIN " + "PRIVATE KEY-----\nTEST\n-----END PRIVATE " + "KEY-----"
    assert findings(sentinel) == []
    assert "private-key" in findings(sentinel.replace("TEST", "QUJDREVGR0g="))

    openssh_sentinel = "-----BEGIN " + "OPENSSH PRIVATE KEY-----\nkey"
    assert findings(openssh_sentinel) == []
    assert "private-key" in findings(openssh_sentinel.replace("\nkey", "\nkey-material"))


def test_secret_scan_detects_nonplaceholder_sensitive_assignment() -> None:
    assignment = "KAGGLE_API_" + "TOKEN=" + "sensitive-value-123456789"
    assert findings(assignment) == ["credential-assignment"]
    legacy_assignment = "KAGGLE_" + "KEY=sensitive-value-123456789"
    assert findings(legacy_assignment) == ["credential-assignment"]


def test_secret_scan_allows_ci_and_documentation_placeholders() -> None:
    assert findings("PASSWORD=integration-only-password") == []
    assert findings("KAGGLE_API_TOKEN=${{ secrets.KAGGLE_API_TOKEN }}") == []


def test_secret_scan_allows_only_exact_historical_region_talk_password_fixture() -> None:
    sentinel = "region-talk-fixture-password-long-enough"
    assert findings(f'PASSWORD="{sentinel}"') == []
    assert findings(f'PASSWORD="{sentinel}-changed"') == ["credential-assignment"]
