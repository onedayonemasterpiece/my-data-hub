from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from heapq import heappop, heappush
from uuid import UUID


class DependencyCycleError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class PendingCommand:
    command_id: UUID
    session_id: UUID
    session_sequence: int
    depends_on: tuple[UUID, ...] = ()


def deterministic_dependency_order(commands: list[PendingCommand]) -> list[PendingCommand]:
    """Topological order with stable session sequence and UUID tie-breaking."""
    by_id = {item.command_id: item for item in commands}
    if len(by_id) != len(commands):
        raise ValueError("duplicate command_id")
    indegree = {item.command_id: 0 for item in commands}
    children: dict[UUID, list[UUID]] = defaultdict(list)
    for item in commands:
        for dependency in item.depends_on:
            if dependency not in by_id:
                continue  # external/already-applied dependency
            indegree[item.command_id] += 1
            children[dependency].append(item.command_id)

    heap: list[tuple[str, int, str, UUID]] = []
    for item in commands:
        if indegree[item.command_id] == 0:
            heappush(
                heap,
                (str(item.session_id), item.session_sequence, str(item.command_id), item.command_id),
            )

    ordered: list[PendingCommand] = []
    while heap:
        _, _, _, command_id = heappop(heap)
        item = by_id[command_id]
        ordered.append(item)
        for child_id in sorted(children.get(command_id, ()), key=str):
            indegree[child_id] -= 1
            if indegree[child_id] == 0:
                child = by_id[child_id]
                heappush(
                    heap,
                    (
                        str(child.session_id),
                        child.session_sequence,
                        str(child.command_id),
                        child.command_id,
                    ),
                )
    if len(ordered) != len(commands):
        cyclic = sorted(str(key) for key, value in indegree.items() if value > 0)
        raise DependencyCycleError("dependency cycle: " + ", ".join(cyclic))
    return ordered
