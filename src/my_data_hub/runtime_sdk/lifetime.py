"""Declared Kaggle provider/runtime lifetime budgets.

These are conservative contract values, not measurements of a particular run.
The provider cutoff is below Kaggle's 12-hour cap, and the notebook process
budget leaves another reserve for pre-entry provider startup variance.
"""

KAGGLE_HARD_CAP_SECONDS = 43_200
PROVIDER_HARD_CUTOFF_RESERVE_SECONDS = 900
KAGGLE_PROVIDER_TIMEOUT_SECONDS = KAGGLE_HARD_CAP_SECONDS - PROVIDER_HARD_CUTOFF_RESERVE_SECONDS
MIN_PROCESS_EXIT_RESERVE_SECONDS = 900
MAX_NOTEBOOK_PROCESS_SECONDS = KAGGLE_PROVIDER_TIMEOUT_SECONDS - MIN_PROCESS_EXIT_RESERVE_SECONDS

# Checkpoint admission budgets are deliberately allocations, not a claim that
# an already-started third-party call can be interrupted at the deadline.  The
# full reserve admits at most two attempts; each attempt must independently
# have its whole allocation left before it may start or resume publication.
# Provider/archive/verifier stage maxima must fit within one attempt allocation.
CHECKPOINT_ATTEMPT_BUDGET_SECONDS = 5_400
MIN_CHECKPOINT_RESERVE_SECONDS = 2 * CHECKPOINT_ATTEMPT_BUDGET_SECONDS
CHECKPOINT_TRANSITION_GUARD_SECONDS = 60
