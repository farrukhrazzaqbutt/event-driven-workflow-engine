"""DAG validation utilities – cycle detection & structural integrity checks."""

from __future__ import annotations

from collections import deque
from typing import Dict, List

from app.models.schemas import DAGDefinition, NodeDefinition


class DAGValidationError(Exception):
    """Raised when the submitted DAG is invalid."""


def validate_dag(dag: DAGDefinition) -> Dict[str, NodeDefinition]:
    """Validate the DAG and return a mapping of node-id -> NodeDefinition.

    Checks performed:
      1. No duplicate node IDs.
      2. Every dependency references an existing node.
      3. The graph is acyclic (Kahn's algorithm – topological sort).

    Raises:
        DAGValidationError: on any validation failure.
    """
    node_map: Dict[str, NodeDefinition] = {}

    # 1. Uniqueness
    for node in dag.nodes:
        if node.id in node_map:
            raise DAGValidationError(f"Duplicate node id: '{node.id}'")
        node_map[node.id] = node

    # 2. Referential integrity
    for node in dag.nodes:
        for dep in node.dependencies:
            if dep not in node_map:
                raise DAGValidationError(
                    f"Node '{node.id}' depends on unknown node '{dep}'"
                )

    # 3. Cycle detection via Kahn's algorithm (uses deque – no while loop)
    in_degree: Dict[str, int] = {nid: 0 for nid in node_map}
    children: Dict[str, List[str]] = {nid: [] for nid in node_map}

    for node in dag.nodes:
        for dep in node.dependencies:
            children[dep].append(node.id)
            in_degree[node.id] += 1

    queue: deque[str] = deque(nid for nid, deg in in_degree.items() if deg == 0)
    sorted_nodes: List[str] = []

    def _drain_queue(q: deque[str]) -> None:
        """Recursively drain the BFS queue without a while loop."""
        if not q:
            return
        current = q.popleft()
        sorted_nodes.append(current)
        for child in children[current]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                q.append(child)
        _drain_queue(q)

    _drain_queue(queue)

    if len(sorted_nodes) != len(node_map):
        raise DAGValidationError("Cycle detected in the workflow DAG.")

    return node_map


def get_root_nodes(dag: DAGDefinition) -> List[str]:
    """Return node IDs that have zero dependencies (entry points)."""
    return [n.id for n in dag.nodes if len(n.dependencies) == 0]
