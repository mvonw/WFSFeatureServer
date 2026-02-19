"""
WFS-T Transaction handler: Insert, Update, Delete operations.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

from models.db_models import Layer
from services.geometry_service import (
    bbox_from_geom,
    geom_to_wkb,
    gml32_to_geom,
    make_transformer,
    reproject_geom,
    wkb_to_geom,
)

_WFS = "http://www.opengis.net/wfs/2.0"
_GML = "http://www.opengis.net/gml/3.2"
_FES = "http://www.opengis.net/fes/2.0"


def execute_transaction(xml_body: bytes, db: sqlite3.Connection) -> str:
    """Parse and execute a WFS Transaction request. Returns response XML."""
    try:
        root = ET.fromstring(xml_body)
    except ET.ParseError as exc:
        return _exception_report("InvalidParameterValue", f"Malformed XML: {exc}")

    # Verify root element
    local = root.tag.split("}")[-1] if "}" in root.tag else root.tag
    if local != "Transaction":
        return _exception_report("OperationNotSupported", f"Expected wfs:Transaction, got {local}")

    inserted: list[tuple[str, str]] = []  # (layer_name, fid)
    total_updated = 0
    total_deleted = 0
    affected_layers: set[int] = set()

    try:
        with db:
            for child in root:
                tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag

                if tag == "Insert":
                    results, layer_ids = _handle_insert(child, db)
                    inserted.extend(results)
                    affected_layers.update(layer_ids)

                elif tag == "Update":
                    count, layer_id = _handle_update(child, db)
                    total_updated += count
                    if layer_id:
                        affected_layers.add(layer_id)

                elif tag == "Delete":
                    count, layer_id = _handle_delete(child, db)
                    total_deleted += count
                    if layer_id:
                        affected_layers.add(layer_id)

            # Update stats for all affected layers
            for layer_id in affected_layers:
                _update_layer_stats(db, layer_id)

    except _WfsError as exc:
        return _exception_report(exc.code, exc.message)
    except Exception as exc:
        return _exception_report("NoApplicableCode", f"Transaction failed: {exc}")

    return _build_response(inserted, total_updated, total_deleted)


# ── Insert ───────────────────────────────────────────────────────────────────

def _handle_insert(
    elem: ET.Element, db: sqlite3.Connection
) -> tuple[list[tuple[str, str]], set[int]]:
    """Process a wfs:Insert element. Returns ([(layer_name, fid), ...], {layer_ids})."""
    inserted = []
    layer_ids = set()

    for feature_elem in elem:
        # Tag is the layer name (possibly namespaced)
        layer_name = feature_elem.tag.split("}")[-1] if "}" in feature_elem.tag else feature_elem.tag
        layer = _get_layer(db, layer_name)
        layer_ids.add(layer.id)

        # Extract gml:id or generate one
        fid = feature_elem.get(f"{{{_GML}}}id") or feature_elem.get("gml:id") or str(uuid.uuid4())
        # Strip "LayerName." prefix if present
        if fid.startswith(f"{layer_name}."):
            fid = fid[len(layer_name) + 1:]

        geometry_wkb = None
        bbox = None
        properties: dict = {}

        for child in feature_elem:
            child_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag

            if child_tag == "geometry" or child_tag == "the_geom":
                # Geometry wrapper — find the actual GML element inside
                gml_elem = _find_gml_geometry(child)
                if gml_elem is not None:
                    geom, srid = gml32_to_geom(gml_elem)
                    # Reproject to storage CRS (4326) if needed
                    transformer = make_transformer(srid, 4326)
                    if transformer:
                        geom = reproject_geom(geom, transformer)
                    geometry_wkb = geom_to_wkb(geom)
                    bbox = bbox_from_geom(geom)
            elif _is_gml_geometry(child):
                # Direct GML geometry element (not wrapped in <geometry>)
                geom, srid = gml32_to_geom(child)
                transformer = make_transformer(srid, 4326)
                if transformer:
                    geom = reproject_geom(geom, transformer)
                geometry_wkb = geom_to_wkb(geom)
                bbox = bbox_from_geom(geom)
            else:
                # Property element
                properties[child_tag] = child.text or ""

        db.execute(
            "INSERT INTO features (layer_id, fid, geometry, properties, bbox_minx, bbox_miny, bbox_maxx, bbox_maxy) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                layer.id,
                fid,
                geometry_wkb,
                json.dumps(properties),
                bbox[0] if bbox else None,
                bbox[1] if bbox else None,
                bbox[2] if bbox else None,
                bbox[3] if bbox else None,
            ),
        )
        inserted.append((layer_name, fid))

    return inserted, layer_ids


# ── Update ───────────────────────────────────────────────────────────────────

def _handle_update(
    elem: ET.Element, db: sqlite3.Connection
) -> tuple[int, int | None]:
    """Process a wfs:Update element. Returns (updated_count, layer_id)."""
    type_name = elem.get("typeName") or elem.get("typeNames") or ""
    layer = _get_layer(db, type_name)

    # Parse properties to update
    prop_updates: dict[str, str | None] = {}
    geom_update = None  # (wkb_bytes, bbox_tuple)

    for prop_elem in elem.findall(f"{{{_WFS}}}Property"):
        ref_elem = prop_elem.find(f"{{{_WFS}}}ValueReference")
        val_elem = prop_elem.find(f"{{{_WFS}}}Value")

        if ref_elem is None or ref_elem.text is None:
            continue
        field_name = ref_elem.text.strip()

        if field_name in ("geometry", "the_geom"):
            if val_elem is not None:
                gml_elem = _find_gml_geometry(val_elem)
                if gml_elem is not None:
                    geom, srid = gml32_to_geom(gml_elem)
                    transformer = make_transformer(srid, 4326)
                    if transformer:
                        geom = reproject_geom(geom, transformer)
                    geom_update = (geom_to_wkb(geom), bbox_from_geom(geom))
        else:
            prop_updates[field_name] = val_elem.text if val_elem is not None else None

    # Parse filter for ResourceId
    fids = _parse_resource_ids(elem, layer.name)
    if not fids:
        return 0, layer.id

    updated = 0
    for fid in fids:
        row = db.execute(
            "SELECT id, properties FROM features WHERE layer_id = ? AND fid = ?",
            (layer.id, fid),
        ).fetchone()
        if not row:
            continue

        sets = []
        params: list = []

        # Merge property updates into existing properties JSON
        if prop_updates:
            existing = json.loads(row["properties"]) if row["properties"] else {}
            existing.update(prop_updates)
            sets.append("properties = ?")
            params.append(json.dumps(existing))

        if geom_update:
            wkb, bbox = geom_update
            sets.append("geometry = ?")
            params.append(wkb)
            sets.append("bbox_minx = ?")
            params.append(bbox[0])
            sets.append("bbox_miny = ?")
            params.append(bbox[1])
            sets.append("bbox_maxx = ?")
            params.append(bbox[2])
            sets.append("bbox_maxy = ?")
            params.append(bbox[3])

        if sets:
            params.append(row["id"])
            db.execute(f"UPDATE features SET {', '.join(sets)} WHERE id = ?", params)
            updated += 1

    return updated, layer.id


# ── Delete ───────────────────────────────────────────────────────────────────

def _handle_delete(
    elem: ET.Element, db: sqlite3.Connection
) -> tuple[int, int | None]:
    """Process a wfs:Delete element. Returns (deleted_count, layer_id)."""
    type_name = elem.get("typeName") or elem.get("typeNames") or ""
    layer = _get_layer(db, type_name)

    fids = _parse_resource_ids(elem, layer.name)
    if not fids:
        return 0, layer.id

    placeholders = ",".join("?" for _ in fids)
    cur = db.execute(
        f"DELETE FROM features WHERE layer_id = ? AND fid IN ({placeholders})",
        [layer.id] + fids,
    )
    return cur.rowcount, layer.id


# ── Helpers ──────────────────────────────────────────────────────────────────

_GML_GEOM_TAGS = {"Point", "LineString", "Polygon", "MultiPoint", "MultiCurve",
                   "MultiSurface", "MultiGeometry"}


def _is_gml_geometry(elem: ET.Element) -> bool:
    """Check if an element is a GML geometry element."""
    local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
    return local in _GML_GEOM_TAGS


def _find_gml_geometry(parent: ET.Element) -> ET.Element | None:
    """Find the first GML geometry child element."""
    for child in parent:
        local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        if local in _GML_GEOM_TAGS:
            return child
    return None


def _get_layer(db: sqlite3.Connection, name: str) -> Layer:
    """Look up a layer by name, raising WfsError if not found."""
    row = db.execute("SELECT * FROM layers WHERE name = ?", (name,)).fetchone()
    if not row:
        raise _WfsError("InvalidParameterValue", f"Unknown feature type: '{name}'")
    return Layer.from_row(row)


def _parse_resource_ids(elem: ET.Element, layer_name: str) -> list[str]:
    """Extract feature IDs from fes:Filter/fes:ResourceId elements."""
    fids = []
    for filt in elem.iter(f"{{{_FES}}}Filter"):
        for rid in filt.iter(f"{{{_FES}}}ResourceId"):
            raw = rid.get("rid", "")
            # ResourceId format: "LayerName.fid" — strip prefix
            if raw.startswith(f"{layer_name}."):
                fids.append(raw[len(layer_name) + 1:])
            else:
                fids.append(raw)
    return fids


def _update_layer_stats(db: sqlite3.Connection, layer_id: int) -> None:
    """Recompute feature_count and bbox for a layer."""
    row = db.execute(
        "SELECT COUNT(*) as cnt, "
        "MIN(bbox_minx) as minx, MIN(bbox_miny) as miny, "
        "MAX(bbox_maxx) as maxx, MAX(bbox_maxy) as maxy "
        "FROM features WHERE layer_id = ?",
        (layer_id,),
    ).fetchone()

    db.execute(
        "UPDATE layers SET feature_count = ?, bbox_minx = ?, bbox_miny = ?, "
        "bbox_maxx = ?, bbox_maxy = ?, updated_at = datetime('now') WHERE id = ?",
        (row["cnt"], row["minx"], row["miny"], row["maxx"], row["maxy"], layer_id),
    )


def _build_response(
    inserted: list[tuple[str, str]], updated: int, deleted: int
) -> str:
    """Build a WFS TransactionResponse XML string."""
    insert_results = ""
    if inserted:
        features_xml = "".join(
            f'<wfs:Feature><fes:ResourceId rid="{layer}.{fid}"/></wfs:Feature>'
            for layer, fid in inserted
        )
        insert_results = f"<wfs:InsertResults>{features_xml}</wfs:InsertResults>"

    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<wfs:TransactionResponse xmlns:wfs="{_WFS}" xmlns:fes="{_FES}" version="2.0.0">'
        "<wfs:TransactionSummary>"
        f"<wfs:totalInserted>{len(inserted)}</wfs:totalInserted>"
        f"<wfs:totalUpdated>{updated}</wfs:totalUpdated>"
        f"<wfs:totalDeleted>{deleted}</wfs:totalDeleted>"
        "</wfs:TransactionSummary>"
        f"{insert_results}"
        "</wfs:TransactionResponse>"
    )


def _exception_report(code: str, text: str) -> str:
    """Build an OWS ExceptionReport XML string."""
    esc = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<ows:ExceptionReport xmlns:ows="http://www.opengis.net/ows/1.1" version="2.0.0">'
        f'<ows:Exception exceptionCode="{code}">'
        f"<ows:ExceptionText>{esc}</ows:ExceptionText>"
        "</ows:Exception>"
        "</ows:ExceptionReport>"
    )


class _WfsError(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)
