"""Template resolution engine for inter-node data passing.

Supports the ``{{ node_name.output_key }}`` syntax.  Before dispatching a
task to a worker the orchestrator calls *resolve* to replace every template
reference with the concrete value produced by a predecessor node.
"""

from __future__ import annotations

import re
from typing import Any, Dict

# Matches  {{ node_id.key }}  with optional whitespace
_TEMPLATE_RE = re.compile(r"\{\{\s*(\w+)\.(\w+)\s*\}\}")


def resolve_value(value: Any, outputs: Dict[str, Dict[str, Any]]) -> Any:
    """Recursively resolve template references inside *value*.

    Parameters
    ----------
    value:
        A string, dict, list, or primitive that may contain template tokens.
    outputs:
        Mapping of ``node_id -> output_dict`` from already-completed nodes.

    Returns
    -------
    The value with all ``{{ node.key }}`` tokens replaced by real data.
    """
    if isinstance(value, str):
        # Full-match replacement (preserves type when the entire string is a ref)
        full_match = _TEMPLATE_RE.fullmatch(value.strip())
        if full_match:
            node_id, key = full_match.group(1), full_match.group(2)
            node_output = outputs.get(node_id, {})
            return node_output.get(key, value)

        # Inline replacement (multiple refs inside a larger string)
        def _replacer(m: re.Match) -> str:
            node_id, key = m.group(1), m.group(2)
            node_output = outputs.get(node_id, {})
            return str(node_output.get(key, m.group(0)))

        return _TEMPLATE_RE.sub(_replacer, value)

    if isinstance(value, dict):
        return {k: resolve_value(v, outputs) for k, v in value.items()}

    if isinstance(value, list):
        return [resolve_value(item, outputs) for item in value]

    return value


def resolve_inputs(
    node_config: Dict[str, Any],
    node_dependencies: list[str],
    all_outputs: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Build the *resolved_inputs* dict for a node about to be dispatched.

    Merges the dependency outputs and resolves any template tokens found in
    the node's ``config`` dictionary.
    """
    # Collect raw outputs from dependencies
    dep_outputs: Dict[str, Any] = {}
    for dep_id in node_dependencies:
        if dep_id in all_outputs:
            dep_outputs[dep_id] = all_outputs[dep_id]

    # Resolve templates inside config
    resolved_config = resolve_value(node_config, all_outputs)

    return {
        "dependency_outputs": dep_outputs,
        "config": resolved_config,
    }
