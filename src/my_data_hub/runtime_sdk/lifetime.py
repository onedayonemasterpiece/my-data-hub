"""Declared Kaggle provider/runtime lifetime budgets.

These are conservative contract values, not measurements of a particular run.
The provider cutoff is below Kaggle's 12-hour cap, and the notebook process
budget leaves another reserve for pre-entry provider startup variance.
"""

KAGGLE_HARD_CAP_SECONDS = 43_200
CANONICAL_RUNTIME_CALLBACK_URL = "https://mcp-datahub.kenigevents.ru/internal/runtime/events"
PROVIDER_HARD_CUTOFF_RESERVE_SECONDS = 900
KAGGLE_PROVIDER_TIMEOUT_SECONDS = KAGGLE_HARD_CAP_SECONDS - PROVIDER_HARD_CUTOFF_RESERVE_SECONDS
MIN_PROCESS_EXIT_RESERVE_SECONDS = 900
MAX_NOTEBOOK_PROCESS_SECONDS = KAGGLE_PROVIDER_TIMEOUT_SECONDS - MIN_PROCESS_EXIT_RESERVE_SECONDS

# Checkpoint admission budgets are deliberately allocations, not a claim that
# an already-started third-party call can be interrupted at the deadline.  The
# full reserve admits at most two attempts; each attempt must independently
# have its whole allocation left before it may start or resume publication.
#
# One production attempt is bounded by two sequential archive commands, one
# independently scheduled verifier run, and a conservative allocation for the
# remaining provider/control-plane upload, readback, and metadata calls.  Keep
# this arithmetic explicit: changing a component without changing the total is
# an architecture-contract failure, not an innocuous timeout tweak.
CHECKPOINT_ARCHIVE_COMMAND_COUNT = 2
CHECKPOINT_ARCHIVE_COMMAND_TIMEOUT_SECONDS = 1_200
CHECKPOINT_VERIFIER_TIMEOUT_SECONDS = 1_800
CHECKPOINT_PROVIDER_IO_BUDGET_SECONDS = 1_200
CHECKPOINT_ATTEMPT_BUDGET_SECONDS = (
    CHECKPOINT_ARCHIVE_COMMAND_COUNT * CHECKPOINT_ARCHIVE_COMMAND_TIMEOUT_SECONDS
    + CHECKPOINT_VERIFIER_TIMEOUT_SECONDS
    + CHECKPOINT_PROVIDER_IO_BUDGET_SECONDS
)
MIN_CHECKPOINT_RESERVE_SECONDS = 2 * CHECKPOINT_ATTEMPT_BUDGET_SECONDS
CHECKPOINT_TRANSITION_GUARD_SECONDS = 60
