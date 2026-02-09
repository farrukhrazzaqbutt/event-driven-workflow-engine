"""API routes for the Workflow Engine.

All endpoints respond synchronously (acknowledge receipt).  Actual workflow
execution happens asynchronously in the background via Kafka.
"""

from __future__ import annotations

import uuid
from typing import Dict

from fastapi import APIRouter, HTTPException

from app.models.schemas import (
    NodeInstance,
    NodeState,
    TriggerRequest,
    WorkflowCreateRequest,
    WorkflowCreateResponse,
    WorkflowExecution,
    WorkflowResultsResponse,
    WorkflowState,
    WorkflowStatusResponse,
    TriggerResponse,
)
from app.services.dag_validator import DAGValidationError, validate_dag
from app.services import orchestrator, state_manager

router = APIRouter()


# ---------------------------------------------------------------------------
# POST /workflow
# ---------------------------------------------------------------------------

@router.post("/workflow", response_model=WorkflowCreateResponse, status_code=201)
async def create_workflow(payload: WorkflowCreateRequest) -> WorkflowCreateResponse:
    """Submit a new workflow JSON for execution.  Returns an execution_id."""

    # Validate the DAG (cycle detection, structural integrity)
    try:
        node_map = validate_dag(payload.dag)
    except DAGValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    execution_id = str(uuid.uuid4())

    # Build node instances from the validated definitions
    nodes: Dict[str, NodeInstance] = {}
    for nid, ndef in node_map.items():
        nodes[nid] = NodeInstance(
            id=nid,
            handler=ndef.handler,
            dependencies=ndef.dependencies,
            config=ndef.config or {},
        )

    execution = WorkflowExecution(
        execution_id=execution_id,
        name=payload.name,
        nodes=nodes,
    )

    await state_manager.save_execution(execution)

    return WorkflowCreateResponse(execution_id=execution_id)


# ---------------------------------------------------------------------------
# POST /workflow/trigger/{execution_id}
# ---------------------------------------------------------------------------

@router.post(
    "/workflow/trigger/{execution_id}",
    response_model=TriggerResponse,
)
async def trigger_workflow(
    execution_id: str,
    body: TriggerRequest | None = None,
) -> TriggerResponse:
    """Trigger the execution of a previously submitted workflow."""

    execution = await state_manager.load_execution(execution_id)
    if execution is None:
        raise HTTPException(status_code=404, detail="Execution not found.")

    if execution.state != WorkflowState.PENDING:
        raise HTTPException(
            status_code=409,
            detail=f"Workflow is already in state '{execution.state.value}'.",
        )

    # Store optional node params
    if body and body.node_params:
        execution.node_params = body.node_params

    execution.state = WorkflowState.RUNNING
    await state_manager.save_execution(execution)

    # Kick off the first batch of ready nodes (root nodes)
    await orchestrator.dispatch_ready_nodes(execution_id)

    return TriggerResponse(execution_id=execution_id)


# ---------------------------------------------------------------------------
# GET /workflows/{execution_id}
# ---------------------------------------------------------------------------

@router.get("/workflows/{execution_id}", response_model=WorkflowStatusResponse)
async def get_workflow_status(execution_id: str) -> WorkflowStatusResponse:
    """Retrieve the current status of a workflow execution."""

    execution = await state_manager.load_execution(execution_id)
    if execution is None:
        raise HTTPException(status_code=404, detail="Execution not found.")

    node_states = {nid: node.state for nid, node in execution.nodes.items()}

    return WorkflowStatusResponse(
        execution_id=execution.execution_id,
        name=execution.name,
        state=execution.state,
        nodes=node_states,
    )


# ---------------------------------------------------------------------------
# GET /workflows/{execution_id}/results
# ---------------------------------------------------------------------------

@router.get(
    "/workflows/{execution_id}/results",
    response_model=WorkflowResultsResponse,
)
async def get_workflow_results(execution_id: str) -> WorkflowResultsResponse:
    """Retrieve the final aggregated output of a completed workflow."""

    execution = await state_manager.load_execution(execution_id)
    if execution is None:
        raise HTTPException(status_code=404, detail="Execution not found.")

    results = await state_manager.collect_outputs(execution_id)

    return WorkflowResultsResponse(
        execution_id=execution.execution_id,
        state=execution.state,
        results=results,
    )
