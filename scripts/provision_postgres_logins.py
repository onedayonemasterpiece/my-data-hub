#!/usr/bin/env python3
"""Provision distinct restricted PostgreSQL LOGIN roles for one deployment.

Passwords are read only from database URLs in the environment.  Reports never include
URLs or passwords.  Existing logins with unexpected direct memberships fail closed.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from urllib.parse import unquote, urlsplit

IDENTITIES = (
    ("MY_DATA_HUB_APPLICATION_DATABASE_URL", "mdh_application"),
    ("MY_DATA_HUB_CONNECTOR_INTAKE_DATABASE_URL", "mdh_connector_intake"),
    ("MY_DATA_HUB_ORCHESTRATOR_DATABASE_URL", "mdh_orchestrator"),
    ("MY_DATA_HUB_MCP_READER_DATABASE_URL", "mdh_mcp_reader"),
    ("MY_DATA_HUB_MCP_REVOCATION_DATABASE_URL", "mdh_authenticator"),
    ("MY_DATA_HUB_CANONICAL_COMMITTER_DATABASE_URL", "mdh_canonical_committer"),
    ("MY_DATA_HUB_BACKUP_DATABASE_URL", "mdh_backup"),
    ("MY_DATA_HUB_MIGRATOR_DATABASE_URL", "mdh_migrator"),
    ("MY_DATA_HUB_MONITORING_DATABASE_URL", "mdh_monitoring"),
    ("MY_DATA_HUB_MIGRATION_OPERATOR_DATABASE_URL", "mdh_migration_operator"),
)


@dataclass(frozen=True, slots=True)
class LoginIdentity:
    environment_name: str
    login: str
    password: str
    group_role: str
    database: str
    hostname: str
    port: int


def load_identity_plan(environment: dict[str, str]) -> tuple[LoginIdentity, ...]:
    admin_raw = environment.get("MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL", "").strip()
    if not admin_raw:
        raise ValueError("MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL is required")
    admin = urlsplit(admin_raw)
    if not admin.hostname or not admin.path.strip("/"):
        raise ValueError("role-admin database URL must include host and database")
    expected_endpoint = (admin.hostname, admin.port or 5432, admin.path.strip("/"))
    group_roles = {group for _, group in IDENTITIES}
    result: list[LoginIdentity] = []
    for environment_name, group_role in IDENTITIES:
        raw = environment.get(environment_name, "").strip()
        if not raw:
            raise ValueError(f"{environment_name} is required")
        parsed = urlsplit(raw)
        login = unquote(parsed.username or "")
        password = unquote(parsed.password or "")
        endpoint = (parsed.hostname or "", parsed.port or 5432, parsed.path.strip("/"))
        if parsed.scheme not in {"postgresql", "postgres"} or endpoint != expected_endpoint:
            raise ValueError(f"{environment_name} must target the role-admin database endpoint")
        if not login or not password:
            raise ValueError(f"{environment_name} must include a login and password")
        if login in group_roles:
            raise ValueError(f"{environment_name} must use a LOGIN distinct from group roles")
        result.append(
            LoginIdentity(
                environment_name=environment_name,
                login=login,
                password=password,
                group_role=group_role,
                database=endpoint[2],
                hostname=endpoint[0],
                port=endpoint[1],
            )
        )
    logins = [item.login for item in result]
    if len(logins) != len(set(logins)):
        raise ValueError("service LOGIN principals must be distinct")
    return tuple(result)


def provision(admin_database_url: str, identities: tuple[LoginIdentity, ...]) -> list[dict[str, str]]:
    import psycopg
    from psycopg import sql

    observations: list[dict[str, str]] = []
    with (
        psycopg.connect(admin_database_url, autocommit=True, connect_timeout=5) as connection,
        connection.cursor() as cursor,
    ):
        for identity in identities:
            managed_comment = f"my-data-hub managed login for {identity.group_role}"
            cursor.execute(
                """
                SELECT oid, shobj_description(oid, 'pg_authid'), rolcanlogin, rolsuper,
                       rolcreatedb, rolcreaterole, rolreplication, rolbypassrls, rolinherit
                FROM pg_roles WHERE rolname = %s
                """,
                (identity.login,),
            )
            existing = cursor.fetchone()
            exists = existing is not None
            if exists:
                assert existing is not None
                role_oid, observed_comment = int(existing[0]), existing[1]
                if observed_comment != managed_comment:
                    raise RuntimeError(f"{identity.login} exists without the expected managed-role marker")
                if not bool(existing[2]) or any(bool(value) for value in existing[3:8]) or not bool(existing[8]):
                    raise RuntimeError(f"{identity.login} existing role attributes are not restricted")
                cursor.execute(
                    """
                    SELECT parent.rolname
                    FROM pg_auth_members membership
                    JOIN pg_roles parent ON parent.oid = membership.roleid
                    JOIN pg_roles member ON member.oid = membership.member
                    WHERE member.rolname = %s
                    ORDER BY parent.rolname
                    """,
                    (identity.login,),
                )
                direct_memberships = {str(row[0]) for row in cursor.fetchall()}
                unexpected = direct_memberships - {identity.group_role}
                if unexpected:
                    raise RuntimeError(
                        f"{identity.login} has unexpected direct memberships: " + ", ".join(sorted(unexpected))
                    )
                cursor.execute(
                    """
                    SELECT
                      EXISTS (SELECT 1 FROM pg_database WHERE datdba = %(role_oid)s)
                      OR EXISTS (SELECT 1 FROM pg_namespace WHERE nspowner = %(role_oid)s)
                      OR EXISTS (SELECT 1 FROM pg_class WHERE relowner = %(role_oid)s)
                      OR EXISTS (SELECT 1 FROM pg_proc WHERE proowner = %(role_oid)s)
                      OR EXISTS (SELECT 1 FROM pg_type WHERE typowner = %(role_oid)s)
                      OR EXISTS (
                        SELECT 1 FROM pg_largeobject_metadata WHERE lomowner = %(role_oid)s
                      )
                      OR EXISTS (
                        SELECT 1 FROM pg_database value,
                          LATERAL aclexplode(value.datacl) acl
                        WHERE value.datacl IS NOT NULL AND acl.grantee = %(role_oid)s
                      )
                      OR EXISTS (
                        SELECT 1 FROM pg_namespace value,
                          LATERAL aclexplode(value.nspacl) acl
                        WHERE value.nspacl IS NOT NULL AND acl.grantee = %(role_oid)s
                      )
                      OR EXISTS (
                        SELECT 1 FROM pg_class value,
                          LATERAL aclexplode(value.relacl) acl
                        WHERE value.relacl IS NOT NULL AND acl.grantee = %(role_oid)s
                      )
                      OR EXISTS (
                        SELECT 1 FROM pg_proc value,
                          LATERAL aclexplode(value.proacl) acl
                        WHERE value.proacl IS NOT NULL AND acl.grantee = %(role_oid)s
                      )
                      OR EXISTS (
                        SELECT 1 FROM pg_type value,
                          LATERAL aclexplode(value.typacl) acl
                        WHERE value.typacl IS NOT NULL AND acl.grantee = %(role_oid)s
                      )
                      OR EXISTS (
                        SELECT 1 FROM pg_default_acl value
                        WHERE value.defaclrole = %(role_oid)s
                           OR EXISTS (
                             SELECT 1 FROM aclexplode(value.defaclacl) acl
                             WHERE acl.grantee = %(role_oid)s
                           )
                      )
                      OR EXISTS (SELECT 1 FROM pg_policy WHERE %(role_oid)s = ANY(polroles))
                    """,
                    {"role_oid": role_oid},
                )
                if bool(cursor.fetchone()[0]):
                    raise RuntimeError(f"{identity.login} owns objects or has direct database privileges")
                cursor.execute(
                    sql.SQL(
                        "ALTER ROLE {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                        "INHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 20 PASSWORD {}"
                    ).format(sql.Identifier(identity.login), sql.Literal(identity.password))
                )
            else:
                cursor.execute(
                    sql.SQL(
                        "CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                        "INHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 20 PASSWORD {}"
                    ).format(sql.Identifier(identity.login), sql.Literal(identity.password))
                )
            cursor.execute(
                sql.SQL("COMMENT ON ROLE {} IS {}").format(sql.Identifier(identity.login), sql.Literal(managed_comment))
            )
            cursor.execute(
                sql.SQL("GRANT {} TO {}").format(sql.Identifier(identity.group_role), sql.Identifier(identity.login))
            )
            observations.append(
                {
                    "environment": identity.environment_name,
                    "login": identity.login,
                    "group_role": identity.group_role,
                    "status": "updated" if exists else "created",
                }
            )
    return observations


def main() -> int:
    admin_database_url = os.getenv("MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL", "").strip()
    try:
        identities = load_identity_plan(dict(os.environ))
        observations = provision(admin_database_url, identities)
    except (ValueError, RuntimeError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2, sort_keys=True))
        return 2
    print(json.dumps({"ok": True, "identities": observations}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
