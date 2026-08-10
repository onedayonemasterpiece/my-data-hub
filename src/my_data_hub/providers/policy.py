from __future__ import annotations

from datetime import datetime

from .models import (
    ControlClass,
    ObservedProviderResource,
    Origin,
    ProviderAction,
    ProviderFingerprint,
    ProviderKind,
    ProviderResource,
    ResourceLease,
    StaleFingerprint,
)


class PolicyDenied(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class ProviderRegistry:
    """In-memory policy projection for tests/adapters, never a canonical registry."""

    def __init__(self) -> None:
        self._resources: dict[tuple[str, str], ProviderResource] = {}

    def resolve_discovery(self, observed: ObservedProviderResource) -> ProviderResource:
        key = (observed.provider, observed.provider_ref)
        registered = self._resources.get(key)
        if registered is not None:
            # Rediscovery and renames cannot weaken an explicit registry decision.
            return registered.model_copy(
                update={
                    "fingerprint": observed.fingerprint,
                    "state": observed.state,
                    "observed_at": observed.observed_at,
                    "private": observed.private,
                }
            )
        external = ProviderResource(
            **observed.model_dump(),
            origin=Origin.EXTERNAL,
            control_class=ControlClass.EXTERNAL_READ_ONLY,
        )
        self._resources[key] = external
        return external

    def register_protected(
        self,
        *,
        provider: str,
        provider_ref: str,
        kind: ProviderKind,
        owner: str,
        fingerprint: ProviderFingerprint | None,
        private: bool | None,
        workload: str,
        observed_at: datetime,
        state: str = "unknown",
        origin: Origin = Origin.ORCHESTRATOR,
    ) -> ProviderResource:
        if origin not in {Origin.ORCHESTRATOR, Origin.MIGRATION}:
            raise PolicyDenied("INVALID_PROTECTED_ORIGIN", "protected registration requires controlled provenance")
        resource = ProviderResource(
            provider=provider,
            provider_ref=provider_ref,
            kind=kind,
            owner=owner,
            origin=origin,
            control_class=ControlClass.ORCHESTRATOR_PROTECTED,
            private=private,
            fingerprint=fingerprint,
            state=state,
            observed_at=observed_at,
            workload=workload,
        )
        key = (provider, provider_ref)
        existing = self._resources.get(key)
        if (
            existing is not None
            and existing.control_class == ControlClass.ORCHESTRATOR_PROTECTED
            and (existing.workload != workload or existing.kind != kind)
        ):
            raise PolicyDenied("PROTECTED_REGISTRATION_CONFLICT", "protected identity is already registered")
        self._resources[key] = resource
        return resource

    def adopt(
        self,
        resource: ProviderResource,
        *,
        target: ControlClass,
        expected_fingerprint: ProviderFingerprint,
    ) -> ProviderResource:
        current = self._resources.get((resource.provider, resource.provider_ref), resource)
        if current.control_class == ControlClass.ORCHESTRATOR_PROTECTED:
            raise PolicyDenied("PROTECTED_RECLASSIFICATION_DENIED", "protected resources cannot be reclassified")
        if target not in {ControlClass.MCP_MANAGED, ControlClass.MCP_EXCHANGE}:
            raise PolicyDenied("INVALID_ADOPTION_TARGET", "adoption target must be explicitly MCP-controlled")
        if current.fingerprint != expected_fingerprint:
            raise StaleFingerprint("provider fingerprint changed before adoption")
        if current.private is not True:
            raise PolicyDenied("PRIVATE_REQUIRED", "only private resources can be adopted")
        adopted = current.model_copy(update={"origin": Origin.MCP, "control_class": target})
        # Revalidate invariants after model_copy (which intentionally skips validation).
        adopted = ProviderResource.model_validate(adopted.model_dump())
        self._resources[(adopted.provider, adopted.provider_ref)] = adopted
        return adopted


class ProviderPolicy:
    _MUTATIONS = frozenset(
        {
            ProviderAction.PUSH,
            ProviderAction.RUN,
            ProviderAction.CREATE_VERSION,
            ProviderAction.DELETE,
        }
    )
    _MCP_MANAGED = frozenset(
        {
            ProviderAction.LIST,
            ProviderAction.READ_STATUS,
            ProviderAction.READ_SOURCE,
            ProviderAction.READ_OUTPUT,
            ProviderAction.PUSH,
            ProviderAction.RUN,
            ProviderAction.DOWNLOAD,
            ProviderAction.CREATE_VERSION,
            ProviderAction.DELETE,
        }
    )
    _EXCHANGE = frozenset(
        {
            ProviderAction.LIST,
            ProviderAction.READ_STATUS,
            ProviderAction.DOWNLOAD,
            ProviderAction.CREATE_VERSION,
            ProviderAction.DELETE,
        }
    )
    _STATUS_ONLY = frozenset({ProviderAction.LIST, ProviderAction.READ_STATUS})

    def authorize(
        self,
        resource: ProviderResource,
        action: ProviderAction,
        *,
        principal: str,
        now: datetime,
        expected_fingerprint: ProviderFingerprint | None = None,
        lease: ResourceLease | None = None,
    ) -> None:
        allowed = {
            ControlClass.ORCHESTRATOR_PROTECTED: self._STATUS_ONLY,
            ControlClass.EXTERNAL_READ_ONLY: self._STATUS_ONLY,
            ControlClass.MCP_MANAGED: self._MCP_MANAGED,
            ControlClass.MCP_EXCHANGE: self._EXCHANGE,
        }.get(resource.control_class, frozenset())
        if action not in allowed:
            code = (
                "PROTECTED_RESOURCE_DENIED"
                if resource.control_class == ControlClass.ORCHESTRATOR_PROTECTED
                else "CONTROL_CLASS_DENIED"
            )
            raise PolicyDenied(code, f"{action.value} is denied for {resource.control_class.value}")
        if action in self._MUTATIONS:
            if expected_fingerprint is None or resource.fingerprint != expected_fingerprint:
                raise StaleFingerprint("exact current provider fingerprint is required")
            if lease is None:
                raise PolicyDenied("LEASE_REQUIRED", "provider mutation requires a resource lease")
            lease.assert_held(principal=principal, provider_ref=resource.provider_ref, now=now)
