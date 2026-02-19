import json
import sqlite3
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from database import get_db
from models.api_models import LayerCreate, LayerResponse, LayerUpdate
from models.db_models import Layer, Feature
from services.geometry_service import geom_to_geojson, wkb_to_geom

router = APIRouter()


def _layer_response(layer: Layer) -> dict:
    bbox = (
        [layer.bbox_minx, layer.bbox_miny, layer.bbox_maxx, layer.bbox_maxy]
        if layer.has_bbox
        else None
    )
    return {
        "id": layer.id,
        "name": layer.name,
        "title": layer.title,
        "description": layer.description,
        "geometry_type": layer.geometry_type,
        "srid": layer.srid,
        "bbox": bbox,
        "feature_count": layer.feature_count,
        "attribute_schema": layer.attribute_schema,
        "created_at": layer.created_at,
        "updated_at": layer.updated_at,
    }


def _get_layer_or_404(layer_id: int, db: sqlite3.Connection) -> Layer:
    row = db.execute("SELECT * FROM layers WHERE id = ?", (layer_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Layer {layer_id} not found")
    return Layer.from_row(row)


# ── List ──────────────────────────────────────────────────────────────────────

@router.get("/layers", response_model=list[LayerResponse])
def list_layers(db: sqlite3.Connection = Depends(get_db)):
    rows = db.execute("SELECT * FROM layers ORDER BY created_at DESC").fetchall()
    return [_layer_response(Layer.from_row(r)) for r in rows]


# ── Create ────────────────────────────────────────────────────────────────────

@router.post("/layers", response_model=LayerResponse, status_code=201)
def create_layer(body: LayerCreate, db: sqlite3.Connection = Depends(get_db)):
    try:
        with db:
            cur = db.execute(
                "INSERT INTO layers (name, title, description) VALUES (?, ?, ?)",
                (body.name, body.title or body.name, body.description),
            )
        layer_id = cur.lastrowid
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail=f"Layer name '{body.name}' already exists")
    layer = _get_layer_or_404(layer_id, db)
    return _layer_response(layer)


# ── Get ───────────────────────────────────────────────────────────────────────

@router.get("/layers/{layer_id}", response_model=LayerResponse)
def get_layer(layer_id: int, db: sqlite3.Connection = Depends(get_db)):
    return _layer_response(_get_layer_or_404(layer_id, db))


# ── Update ────────────────────────────────────────────────────────────────────

@router.patch("/layers/{layer_id}", response_model=LayerResponse)
def update_layer(layer_id: int, body: LayerUpdate, db: sqlite3.Connection = Depends(get_db)):
    _get_layer_or_404(layer_id, db)
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if not updates:
        return _layer_response(_get_layer_or_404(layer_id, db))
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    set_clause += ", updated_at = datetime('now')"
    with db:
        db.execute(
            f"UPDATE layers SET {set_clause} WHERE id = ?",
            (*updates.values(), layer_id),
        )
    return _layer_response(_get_layer_or_404(layer_id, db))


# ── Delete ────────────────────────────────────────────────────────────────────

@router.delete("/layers/{layer_id}", status_code=204)
def delete_layer(layer_id: int, db: sqlite3.Connection = Depends(get_db)):
    _get_layer_or_404(layer_id, db)
    with db:
        db.execute("DELETE FROM layers WHERE id = ?", (layer_id,))


# ── Feature preview (GeoJSON) ─────────────────────────────────────────────────

@router.get("/layers/{layer_id}/features/preview")
def feature_preview(
    layer_id: int,
    max: int = Query(default=1000, le=5000),
    db: sqlite3.Connection = Depends(get_db),
):
    _get_layer_or_404(layer_id, db)
    rows = db.execute(
        "SELECT * FROM features WHERE layer_id = ? LIMIT ?", (layer_id, max)
    ).fetchall()

    features = []
    for row in rows:
        feat = Feature.from_row(row)
        geom_json = None
        if feat.geometry:
            try:
                geom = wkb_to_geom(bytes(feat.geometry))
                geom_json = geom_to_geojson(geom)
            except Exception:
                pass
        features.append({
            "type": "Feature",
            "id": feat.fid,
            "geometry": geom_json,
            "properties": feat.properties,
        })

    return JSONResponse({
        "type": "FeatureCollection",
        "features": features,
    })
