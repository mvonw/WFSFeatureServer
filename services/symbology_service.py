"""
Rule-based symbology evaluation.
Mirrors the client-side ruleMatches() logic in admin.html exactly
so server-side and client-side rendering agree.
"""
from __future__ import annotations

import json
from typing import Any

from models.db_models import SymbologyRule


DEFAULT_STYLE = {
    "fill_color": "#3388ff",
    "fill_opacity": 0.6,
    "stroke_color": "#ffffff",
    "stroke_width": 1.5,
    "point_radius": 6.0,
}


def evaluate_rules(
    rules: list[SymbologyRule],
    properties: dict[str, Any],
) -> SymbologyRule | None:
    """
    Return the first matching rule in rule_order, or the default rule.
    Returns None if no rules exist.
    """
    if not rules:
        return None
    sorted_rules = sorted(rules, key=lambda r: r.rule_order)
    default_rule = next((r for r in sorted_rules if r.is_default), None)
    for rule in sorted_rules:
        if rule.is_default:
            continue
        if _matches(rule, properties):
            return rule
    return default_rule


def _matches(rule: SymbologyRule, props: dict[str, Any]) -> bool:
    if not rule.filter_field:
        return True
    val = props.get(rule.filter_field)
    rv = rule.filter_value
    op = rule.filter_operator

    if op == "is_null":
        return val is None or val == ""
    if val is None:
        return False

    if op == "eq":
        return str(val) == str(rv)
    elif op == "neq":
        return str(val) != str(rv)
    elif op == "contains":
        return str(rv) in str(val)
    elif op == "in":
        try:
            allowed = json.loads(rv or "[]")
            return str(val) in [str(a) for a in allowed]
        except (json.JSONDecodeError, TypeError):
            return False
    else:
        # Numeric comparisons
        try:
            num_val = float(val)
            num_rv = float(rv)
        except (TypeError, ValueError):
            return False
        if op == "gt":
            return num_val > num_rv
        elif op == "gte":
            return num_val >= num_rv
        elif op == "lt":
            return num_val < num_rv
        elif op == "lte":
            return num_val <= num_rv
    return False
