"""Tests for the template resolution engine."""

from app.services.template_resolver import resolve_value, resolve_inputs


def test_simple_string_replacement() -> None:
    outputs = {"nodeA": {"name": "Alice"}}
    result = resolve_value("{{ nodeA.name }}", outputs)
    assert result == "Alice"


def test_inline_replacement() -> None:
    outputs = {"nodeA": {"name": "Alice"}, "nodeB": {"age": 30}}
    result = resolve_value("Hello {{ nodeA.name }}, age {{ nodeB.age }}!", outputs)
    assert result == "Hello Alice, age 30!"


def test_full_match_preserves_type() -> None:
    outputs = {"nodeA": {"count": 42}}
    result = resolve_value("{{ nodeA.count }}", outputs)
    assert result == 42
    assert isinstance(result, int)


def test_dict_resolution() -> None:
    outputs = {"nodeA": {"url": "http://example.com"}}
    config = {"endpoint": "{{ nodeA.url }}", "timeout": 30}
    result = resolve_value(config, outputs)
    assert result == {"endpoint": "http://example.com", "timeout": 30}


def test_list_resolution() -> None:
    outputs = {"nodeA": {"val": "X"}}
    result = resolve_value(["{{ nodeA.val }}", "static"], outputs)
    assert result == ["X", "static"]


def test_unresolved_template_kept() -> None:
    result = resolve_value("{{ missing.key }}", {})
    assert result == "{{ missing.key }}"


def test_resolve_inputs_merges_deps() -> None:
    all_outputs = {
        "A": {"x": 1},
        "B": {"y": 2},
    }
    resolved = resolve_inputs(
        node_config={"url": "{{ A.x }}"},
        node_dependencies=["A", "B"],
        all_outputs=all_outputs,
    )
    assert resolved["dependency_outputs"]["A"] == {"x": 1}
    assert resolved["dependency_outputs"]["B"] == {"y": 2}
    assert resolved["config"]["url"] == 1
