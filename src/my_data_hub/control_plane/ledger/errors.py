class ControlLedgerError(RuntimeError):
    """Base class for fail-closed control-ledger errors."""


class IdempotencyConflict(ControlLedgerError):
    pass


class MasterAdmissionRejected(ControlLedgerError):
    """A distinct master request cannot safely allocate the next epoch."""


class StaleRuntimeEvent(ControlLedgerError):
    pass


class EventRejected(ControlLedgerError):
    pass


class LeaseRejected(ControlLedgerError):
    pass


class MigrationError(ControlLedgerError):
    pass
