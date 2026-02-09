"""Tests for DAG validation – cycle detection, structural integrity, duplicates."""

import pytest

from app.models.schemas import DAGDefinition, NodeDefinition
from app.services.dag_validator import DAGValidationError, validate_dag, get_root_nodes


# ---------------------------------------------------------------------------
# Valid DAGs
# ---------------------------------------------------------------------------

def test_valid_linear_dag() -> None:
    """A -> B -> C should pass validation."""
    dag = DAGDefinition(nodes=[
        NodeDefinition(id="A", handler="input", dependencies=[]),
        NodeDefinition(id="B", handler="call_external_service", dependencies=["A"]),
        NodeDefinition(id="C", handler="output", dependencies=["B"]),
    ])
    node_map = validate_dag(dag)
    assert set(node_map.keys()) == {"A", "B", "C"}


def test_valid_fan_out_fan_in_dag() -> None:
    """A -> B, A -> C, B+C -> D should pass."""
    dag = DAGDefinition(nodes=[
        NodeDefinition(id="A", handler="input", dependencies=[]),
        NodeDefinition(id="B", handler="call_external_service", dependencies=["A"]),
        NodeDefinition(id="C", handler="call_external_service", dependencies=["A"]),
        NodeDefinition(id="D", handler="output", dependencies=["B", "C"]),
    ])
    node_map = validate_dag(dag)
    assert len(node_map) == 4


def test_valid_single_node() -> None:
    dag = DAGDefinition(nodes=[
        NodeDefinition(id="solo", handler="input", dependencies=[]),
    ])
    node_map = validate_dag(dag)
    assert "solo" in node_map


# ---------------------------------------------------------------------------
# Invalid DAGs
# ---------------------------------------------------------------------------

def test_cycle_detection_simple() -> None:
    """A -> B -> A should be rejected."""
    dag = DAGDefinition(nodes=[
        NodeDefinition(id="A", handler="input", dependencies=["B"]),
        NodeDefinition(id="B", handler="output", dependencies=["A"]),
    ])
    with pytest.raises(DAGValidationError, match="Cycle detected"):
        validate_dag(dag)


def test_cycle_detection_three_nodes() -> None:
    """A -> B -> C -> A should be rejected."""
    dag = DAGDefinition(nodes=[
        NodeDefinition(id="A", handler="input", dependencies=["C"]),
        NodeDefinition(id="B", handler="call_external_service", dependencies=["A"]),
        NodeDefinition(id="C", handler="output", dependencies=["B"]),
    ])
    with pytest.raises(DAGValidationError, match="Cycle detected"):
        validate_dag(dag)


def test_duplicate_node_id() -> None:
    dag = DAGDefinition(nodes=[
        NodeDefinition(id="A", handler="input", dependencies=[]),
        NodeDefinition(id="A", handler="output", dependencies=[]),
    ])
    with pytest.raises(DAGValidationError, match="Duplicate node id"):
        validate_dag(dag)


def test_missing_dependency() -> None:
    dag = DAGDefinition(nodes=[
        NodeDefinition(id="A", handler="input", dependencies=["ghost"]),
    ])
    with pytest.raises(DAGValidationError, match="unknown node 'ghost'"):
        validate_dag(dag)


# ---------------------------------------------------------------------------
# Root node detection
# ---------------------------------------------------------------------------

def test_get_root_nodes() -> None:
    dag = DAGDefinition(nodes=[
        NodeDefinition(id="A", handler="input", dependencies=[]),
        NodeDefinition(id="B", handler="call_external_service", dependencies=["A"]),
        NodeDefinition(id="C", handler="input", dependencies=[]),
    ])
    roots = get_root_nodes(dag)
    assert set(roots) == {"A", "C"}
