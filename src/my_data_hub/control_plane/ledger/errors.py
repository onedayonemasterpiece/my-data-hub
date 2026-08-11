class ControlLedgerError(RuntimeError):
    """Base class for fail-closed control-ledger errors."""


class IdempotencyConflict(ControlLedgerError):
    pass


class StaleRuntimeEvent(ControlLedgerError):
    pass


class EventRejected(ControlLedgerError):
    pass


class LeaseRejected(ControlLedgerError):
    pass


class MigrationError(ControlLedgerError):
    pass
