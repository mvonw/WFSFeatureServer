"""
OGC WFS 2.0.0 response builders.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from config import settings
from models.db_models import Feature, Layer
from services.geometry_service import (
    geom_to_geojson,
    wkb_to_geom,
    wkb_to_gml32,
)

# ── Jinja2 environment ────────────────────────────────────────────────────────

_GML_GEOM_MAP = {
    "Point": "gml:PointPropertyType",
    "MultiPoint": "gml:MultiPointPropertyType",
    "LineString": "gml:CurvePropertyType",
    "MultiLineString": "gml:MultiCurvePropertyType",
    "Polygon": "gml:SurfacePropertyType",
    "MultiPolygon": "gml:MultiSurfacePropertyType",
    "GeometryCollection": "gml:GeometryPropertyType",
}

_XSD_TYPE_MAP = {
    "String": "xsd:string",
    "Integer": "xsd:long",
    "Real": "xsd:double",
    "Date": "xsd:date",
}

templates_dir = Path(__file__).parent.parent / "templates"
_jinja_env = Environment(
    loader=FileSystemLoader(str(templates_dir)),
    autoescape=select_autoescape(["xml"]),
)


def _gml_geom_type(layer: Layer) -> str:
    return _GML_GEOM_MAP.get(layer.geometry_type, "gml:GeometryPropertyType")


def _xsd_type(field_type: str) -> str:
    return _XSD_TYPE_MAP.get(field_type, "xsd:string")


_jinja_env.filters["gml_geom_type"] = _gml_geom_type
_jinja_env.filters["xsd_type"] = _xsd_type


# ── GetCapabilities ───────────────────────────────────────────────────────────

def build_capabilities(db: sqlite3.Connection) -> str:
    rows = db.execute("SELECT * FROM layers ORDER BY name").fetchall()
    layers = [Layer.from_row(r) for r in rows]
    tmpl = _jinja_env.get_template("wfs_capabilities.xml")
    return tmpl.render(layers=layers, settings=settings, service_url=settings.service_url)


# ── DescribeFeatureType ───────────────────────────────────────────────────────

def build_describe(typenames: str | None, db: sqlite3.Connection) -> str:
    if typenames:
        names = [n.strip() for n in typenames.replace(",", " ").split()]
        rows = db.execute(
            f"SELECT * FROM layers WHERE name IN ({','.join('?' for _ in names)})",
            names,
        ).fetchall()
    else:
        rows = db.execute("SELECT * FROM layers ORDER BY name").fetchall()

    layers = [Layer.from_row(r) for r in rows]
    tmpl = _jinja_env.get_template("wfs_describe.xml")
    return tmpl.render(layers=layers)


# ── GetFeature ────────────────────────────────────────────────────────────────

def build_get_feature_geojson(
    typenames: str,
    db: sqlite3.Connection,
    bbox: tuple[float, float, float, float] | None = None,
    count: int | None = None,
    startindex: int = 0,
    max_features: int = 10000,
) -> dict[str, Any]:
    """Returns a GeoJSON FeatureCollection dict."""
    name = typenames.strip().split()[0]
    layer_row = db.execute("SELECT * FROM layers WHERE name = ?", (name,)).fetchone()
    if not layer_row:
        return {"type": "FeatureCollection", "features": [], "numberMatched": 0, "numberReturned": 0}
    layer = Layer.from_row(layer_row)

    features_rows, total = _query_features(db, layer, bbox, count, startindex, max_features)
    geojson_features = []
    for feat in features_rows:
        geom_json = None
        if feat.geometry:
            try:
                geom = wkb_to_geom(bytes(feat.geometry))
                geom_json = geom_to_geojson(geom)
            except Exception:
                pass
        geojson_features.append({
            "type": "Feature",
            "id": f"{layer.name}.{feat.fid}",
            "geometry": geom_json,
            "properties": feat.properties,
        })

    return {
        "type": "FeatureCollection",
        "numberMatched": total,
        "numberReturned": len(geojson_features),
        "timeStamp": _now_iso(),
        "features": geojson_features,
    }


def build_get_feature_gml(
    typenames: str,
    db: sqlite3.Connection,
    bbox: tuple[float, float, float, float] | None = None,
    count: int | None = None,
    startindex: int = 0,
    max_features: int = 10000,
) -> str:
    """Returns a GML 3.2 WFS FeatureCollection string."""
    name = typenames.strip().split()[0]
    layer_row = db.execute("SELECT * FROM layers WHERE name = ?", (name,)).fetchone()
    if not layer_row:
        return _empty_gml_collection()
    layer = Layer.from_row(layer_row)

    features_rows, total = _query_features(db, layer, bbox, count, startindex, max_features)
    srs = f"urn:ogc:def:crs:EPSG::{layer.srid}"
    members = []
    for feat in features_rows:
        props_xml = "".join(
            f"<{_safe_tag(k)}>{_esc(v)}</{_safe_tag(k)}>"
            for k, v in feat.properties.items()
        )
        geom_xml = ""
        if feat.geometry:
            try:
                geom_xml = f"<geometry>{wkb_to_gml32(bytes(feat.geometry), layer.srid)}</geometry>"
            except Exception:
                pass
        members.append(
            f'<wfs:member><{layer.name} gml:id="{layer.name}.{feat.fid}">'
            f"{geom_xml}{props_xml}"
            f"</{layer.name}></wfs:member>"
        )

    bbox_xml = _bbox_gml(layer, srs)
    members_str = "\n".join(members)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<wfs:FeatureCollection '
        f'xmlns:wfs="http://www.opengis.net/wfs/2.0" '
        f'xmlns:gml="http://www.opengis.net/gml/3.2" '
        f'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        f'numberMatched="{total}" numberReturned="{len(features_rows)}" '
        f'timeStamp="{_now_iso()}">'
        f"{bbox_xml}"
        f"{members_str}"
        f"</wfs:FeatureCollection>"
    )


def _empty_gml_collection() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" '
        f'xmlns:gml="http://www.opengis.net/gml/3.2" '
        f'numberMatched="0" numberReturned="0" timeStamp="{_now_iso()}"/>'
    )


# ── Feature query ─────────────────────────────────────────────────────────────

def _query_features(
    db: sqlite3.Connection,
    layer: Layer,
    bbox: tuple[float, float, float, float] | None,
    count: int | None,
    startindex: int,
    max_features: int,
) -> tuple[list[Feature], int]:
    base_sql = "FROM features WHERE layer_id = ?"
    params: list[Any] = [layer.id]

    if bbox:
        minx, miny, maxx, maxy = bbox
        base_sql += (
            " AND NOT (bbox_maxx < ? OR bbox_minx > ? OR bbox_maxy < ? OR bbox_miny > ?)"
        )
        params.extend([minx, maxx, miny, maxy])

    total_row = db.execute(f"SELECT COUNT(*) {base_sql}", params).fetchone()
    total = total_row[0] if total_row else 0

    limit = min(count if count is not None else max_features, max_features)
    rows = db.execute(
        f"SELECT * {base_sql} ORDER BY id LIMIT ? OFFSET ?",
        params + [limit, startindex],
    ).fetchall()
    return [Feature.from_row(r) for r in rows], total


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _safe_tag(name: str) -> str:
    """Make a string safe as an XML tag."""
    name = "".join(c if c.isalnum() or c in ("_", "-", ".") else "_" for c in str(name))
    if name and name[0].isdigit():
        name = "_" + name
    return name or "field"


def _esc(v: Any) -> str:
    if v is None:
        return ""
    s = str(v)
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _bbox_gml(layer: Layer, srs: str) -> str:
    if not layer.has_bbox:
        return ""
    # For EPSG:4326 the axis order is lat,lon (Y,X) — swap min/max accordingly.
    swap = "EPSG::4326" in srs
    lc = f"{layer.bbox_miny} {layer.bbox_minx}" if swap else f"{layer.bbox_minx} {layer.bbox_miny}"
    uc = f"{layer.bbox_maxy} {layer.bbox_maxx}" if swap else f"{layer.bbox_maxx} {layer.bbox_maxy}"
    return (
        f'<gml:boundedBy><gml:Envelope srsName="{srs}">'
        f"<gml:lowerCorner>{lc}</gml:lowerCorner>"
        f"<gml:upperCorner>{uc}</gml:upperCorner>"
        f"</gml:Envelope></gml:boundedBy>"
    )
