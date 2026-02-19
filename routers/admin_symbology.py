import sqlite3
from fastapi import APIRouter, Depends, HTTPException

from database import get_db
from models.api_models import SymbologyRuleCreate, SymbologyRuleResponse, SymbologyReorderRequest
from models.db_models import SymbologyRule

router = APIRouter()


def _get_layer_or_404(layer_id: int, db: sqlite3.Connection):
    if not db.execute("SELECT id FROM layers WHERE id = ?", (layer_id,)).fetchone():
        raise HTTPException(status_code=404, detail=f"Layer {layer_id} not found")


def _rule_response(rule: SymbologyRule) -> dict:
    return {
        "id": rule.id,
        "layer_id": rule.layer_id,
        "rule_order": rule.rule_order,
        "label": rule.label,
        "filter_field": rule.filter_field,
        "filter_operator": rule.filter_operator,
        "filter_value": rule.filter_value,
        "fill_color": rule.fill_color,
        "fill_opacity": rule.fill_opacity,
        "stroke_color": rule.stroke_color,
        "stroke_width": rule.stroke_width,
        "point_radius": rule.point_radius,
        "is_default": rule.is_default,
    }


# ── List ──────────────────────────────────────────────────────────────────────

@router.get("/layers/{layer_id}/symbology", response_model=list[SymbologyRuleResponse])
def list_rules(layer_id: int, db: sqlite3.Connection = Depends(get_db)):
    _get_layer_or_404(layer_id, db)
    rows = db.execute(
        "SELECT * FROM symbology_rules WHERE layer_id = ? ORDER BY rule_order ASC",
        (layer_id,),
    ).fetchall()
    return [_rule_response(SymbologyRule.from_row(r)) for r in rows]


# ── Create ────────────────────────────────────────────────────────────────────

@router.post("/layers/{layer_id}/symbology", response_model=SymbologyRuleResponse, status_code=201)
def create_rule(layer_id: int, body: SymbologyRuleCreate, db: sqlite3.Connection = Depends(get_db)):
    _get_layer_or_404(layer_id, db)
    with db:
        cur = db.execute(
            """INSERT INTO symbology_rules
               (layer_id, rule_order, label, filter_field, filter_operator, filter_value,
                fill_color, fill_opacity, stroke_color, stroke_width, point_radius, is_default)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                layer_id, body.rule_order, body.label, body.filter_field,
                body.filter_operator, body.filter_value,
                body.fill_color, body.fill_opacity, body.stroke_color,
                body.stroke_width, body.point_radius, int(body.is_default),
            ),
        )
    row = db.execute("SELECT * FROM symbology_rules WHERE id = ?", (cur.lastrowid,)).fetchone()
    return _rule_response(SymbologyRule.from_row(row))


# ── Bulk replace ──────────────────────────────────────────────────────────────

@router.put("/layers/{layer_id}/symbology", response_model=list[SymbologyRuleResponse])
def replace_rules(layer_id: int, body: list[SymbologyRuleCreate], db: sqlite3.Connection = Depends(get_db)):
    _get_layer_or_404(layer_id, db)
    with db:
        db.execute("DELETE FROM symbology_rules WHERE layer_id = ?", (layer_id,))
        for i, rule in enumerate(body):
            db.execute(
                """INSERT INTO symbology_rules
                   (layer_id, rule_order, label, filter_field, filter_operator, filter_value,
                    fill_color, fill_opacity, stroke_color, stroke_width, point_radius, is_default)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    layer_id, i, rule.label, rule.filter_field,
                    rule.filter_operator, rule.filter_value,
                    rule.fill_color, rule.fill_opacity, rule.stroke_color,
                    rule.stroke_width, rule.point_radius, int(rule.is_default),
                ),
            )
    return list_rules(layer_id, db)


# ── Update single ─────────────────────────────────────────────────────────────

@router.put("/layers/{layer_id}/symbology/{rule_id}", response_model=SymbologyRuleResponse)
def update_rule(
    layer_id: int, rule_id: int, body: SymbologyRuleCreate, db: sqlite3.Connection = Depends(get_db)
):
    _get_layer_or_404(layer_id, db)
    if not db.execute(
        "SELECT id FROM symbology_rules WHERE id = ? AND layer_id = ?", (rule_id, layer_id)
    ).fetchone():
        raise HTTPException(status_code=404, detail=f"Rule {rule_id} not found")
    with db:
        db.execute(
            """UPDATE symbology_rules SET
               rule_order=?, label=?, filter_field=?, filter_operator=?, filter_value=?,
               fill_color=?, fill_opacity=?, stroke_color=?, stroke_width=?, point_radius=?, is_default=?
               WHERE id = ? AND layer_id = ?""",
            (
                body.rule_order, body.label, body.filter_field,
                body.filter_operator, body.filter_value,
                body.fill_color, body.fill_opacity, body.stroke_color,
                body.stroke_width, body.point_radius, int(body.is_default),
                rule_id, layer_id,
            ),
        )
    row = db.execute("SELECT * FROM symbology_rules WHERE id = ?", (rule_id,)).fetchone()
    return _rule_response(SymbologyRule.from_row(row))


# ── Delete ────────────────────────────────────────────────────────────────────

@router.delete("/layers/{layer_id}/symbology/{rule_id}", status_code=204)
def delete_rule(layer_id: int, rule_id: int, db: sqlite3.Connection = Depends(get_db)):
    _get_layer_or_404(layer_id, db)
    with db:
        db.execute(
            "DELETE FROM symbology_rules WHERE id = ? AND layer_id = ?", (rule_id, layer_id)
        )


# ── Reorder ───────────────────────────────────────────────────────────────────

@router.post("/layers/{layer_id}/symbology/reorder", response_model=list[SymbologyRuleResponse])
def reorder_rules(
    layer_id: int, body: SymbologyReorderRequest, db: sqlite3.Connection = Depends(get_db)
):
    _get_layer_or_404(layer_id, db)
    with db:
        for i, rule_id in enumerate(body.order):
            db.execute(
                "UPDATE symbology_rules SET rule_order = ? WHERE id = ? AND layer_id = ?",
                (i, rule_id, layer_id),
            )
    return list_rules(layer_id, db)
