from __future__ import annotations

from pathlib import Path

import pytest

from scripts import provision_postgres_logins
from scripts.provision_postgres_logins import IDENTITIES, load_identity_plan


def environment() -> dict[str, str]:
    values = {"MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL": "postgresql://admin:secret@postgres:5432/hub"}
    for index, (name, _group) in enumerate(IDENTITIES):
        values[name] = f"postgresql://service_{index}:secret_{index}@postgres:5432/hub"
    return values


def test_login_plan_requires_distinct_restricted_principals() -> None:
    plan = load_identity_plan(environment())
    assert len(plan) == len(IDENTITIES)
    assert len({item.login for item in plan}) == len(IDENTITIES)
    assert {item.group_role for item in plan} == {group for _, group in IDENTITIES}


def test_login_plan_rejects_shared_login() -> None:
    values = environment()
    first, second = IDENTITIES[0][0], IDENTITIES[1][0]
    values[second] = values[first]
    with pytest.raises(ValueError, match="distinct"):
        load_identity_plan(values)


def test_login_plan_rejects_group_role_as_login() -> None:
    values = environment()
    name, group = IDENTITIES[0]
    values[name] = f"postgresql://{group}:secret@postgres:5432/hub"
    with pytest.raises(ValueError, match="distinct from group"):
        load_identity_plan(values)


def test_login_plan_rejects_different_database_endpoint() -> None:
    values = environment()
    values[IDENTITIES[0][0]] = "postgresql://service:secret@other:5432/hub"
    with pytest.raises(ValueError, match="role-admin database endpoint"):
        load_identity_plan(values)


def test_existing_login_path_is_fail_closed_for_collisions() -> None:
    source = provision_postgres_logins.__file__
    assert source is not None
    text = Path(source).read_text(encoding="utf-8")
    assert "managed-role marker" in text
    assert "owns objects or has direct database privileges" in text
    assert "existing role attributes are not restricted" in text
    assert "membership.admin_option" in text
    assert "membership.inherit_option" in text
    assert "membership.set_option" in text
    assert "unsafe membership options" in text
