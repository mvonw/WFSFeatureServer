"""
File import pipeline for GeoJSON, Shapefile (.zip), GeoPackage (.gpkg), CSV.
All formats are normalised to WGS84 (EPSG:4326) WKB and stored in the
features table with per-feature bbox columns.
"""
from __future__ import annotations

import csv
import json
import shutil
import sqlite3
import tempfile
import uuid
import zipfile
from pathlib import Path
from typing import Any

import shapely.geometry
from shapely.geometry.base import BaseGeometry

from models.api_models import ImportResult
from services.geometry_service import (
    bbox_from_geom,
    geom_to_wkb,
    infer_schema,
    make_transformer,
    reproject_geom,
)


# ── Public entry point ────────────────────────────────────────────────────────

def import_file(
    file_path: Path,
    layer_id: int,
    db: sqlite3.Connection,
    source_srid: int = 4326,
    lat_field: str | None = None,
    lon_field: str | None = None,
    replace_existing: bool = False,
) -> ImportResult:
    ext = file_path.suffix.lower()

    if replace_existing:
        db.execute("DELETE FROM features WHERE layer_id = ?", (layer_id,))

    if ext in (".geojson", ".json"):
        result = _import_geojson(file_path, layer_id, db, source_srid)
    elif ext == ".zip":
        result = _import_shapefile_zip(file_path, layer_id, db, source_srid)
    elif ext == ".gpkg":
        result = _import_geopackage(file_path, layer_id, db, source_srid)
    elif ext == ".csv":
        result = _import_csv(file_path, layer_id, db, source_srid, lat_field, lon_field)
    else:
        raise ValueError(f"Unsupported file format: {ext}")

    _update_layer_stats(layer_id, db)
    return result


# ── GeoJSON ───────────────────────────────────────────────────────────────────

def _import_geojson(
    path: Path, layer_id: int, db: sqlite3.Connection, source_srid: int
) -> ImportResult:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    features_raw = []
    if data.get("type") == "FeatureCollection":
        features_raw = data.get("features", [])
    elif data.get("type") == "Feature":
        features_raw = [data]
    else:
        raise ValueError("GeoJSON must be a FeatureCollection or Feature")

    transformer = make_transformer(source_srid)
    records, errors, sample_props = [], [], []

    for i, feat in enumerate(features_raw):
        try:
            geom_data = feat.get("geometry")
            if not geom_data:
                raise ValueError("Null geometry")
            geom = shapely.geometry.shape(geom_data)
            if transformer:
                geom = reproject_geom(geom, transformer)
            props = feat.get("properties") or {}
            rec = _make_record(layer_id, geom, props, feat.get("id"))
            records.append(rec)
            if len(sample_props) < 100:
                sample_props.append(props)
        except Exception as e:
            errors.append(f"Feature {i}: {e}")

    imported, failed, batch_errors = _batch_insert(db, records)
    errors.extend(batch_errors)
    _update_attribute_schema(layer_id, db, sample_props)
    bbox = _compute_bbox(records)
    return ImportResult(features_imported=imported, features_failed=failed + len(errors) - len(batch_errors), errors=errors, bbox=bbox)


# ── Shapefile ZIP ─────────────────────────────────────────────────────────────

def _import_shapefile_zip(
    path: Path, layer_id: int, db: sqlite3.Connection, source_srid: int
) -> ImportResult:
    tmpdir = Path(tempfile.mkdtemp())
    try:
        with zipfile.ZipFile(path) as zf:
            zf.extractall(tmpdir)

        shp_files = list(tmpdir.rglob("*.shp"))
        if not shp_files:
            raise ValueError("No .shp file found in ZIP archive")
        shp_path = shp_files[0]

        return _import_via_fiona(shp_path, layer_id, db, source_srid)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ── GeoPackage ────────────────────────────────────────────────────────────────

def _import_geopackage(
    path: Path, layer_id: int, db: sqlite3.Connection, source_srid: int
) -> ImportResult:
    return _import_via_fiona(path, layer_id, db, source_srid)


# ── Fiona-based import (Shapefile + GPKG) ─────────────────────────────────────

def _import_via_fiona(
    path: Path, layer_id: int, db: sqlite3.Connection, source_srid: int
) -> ImportResult:
    try:
        import fiona
        import fiona.crs
        from pyproj import CRS
    except ImportError:
        raise ImportError("fiona is required to import Shapefile and GeoPackage files")

    records, errors, sample_props = [], [], []

    with fiona.open(str(path)) as src:
        # Determine source CRS
        detected_srid = source_srid
        if src.crs:
            try:
                detected_crs = CRS.from_user_input(src.crs)
                detected_srid = detected_crs.to_epsg() or source_srid
            except Exception:
                pass

        transformer = make_transformer(detected_srid)

        for i, feat in enumerate(src):
            try:
                geom_data = feat.get("geometry")
                if not geom_data:
                    raise ValueError("Null geometry")
                geom = shapely.geometry.shape(geom_data)
                if transformer:
                    geom = reproject_geom(geom, transformer)
                props = dict(feat.get("properties") or {})
                # fiona already returns None for null values in 1.9+
                rec = _make_record(layer_id, geom, props, feat.get("id"))
                records.append(rec)
                if len(sample_props) < 100:
                    sample_props.append(props)
            except Exception as e:
                errors.append(f"Feature {i}: {e}")

    imported, failed, batch_errors = _batch_insert(db, records)
    errors.extend(batch_errors)
    _update_attribute_schema(layer_id, db, sample_props)
    bbox = _compute_bbox(records)
    return ImportResult(features_imported=imported, features_failed=failed, errors=errors, bbox=bbox)


# ── CSV ───────────────────────────────────────────────────────────────────────

_LAT_NAMES = {"lat", "latitude", "y", "northing", "ylat"}
_LON_NAMES = {"lon", "lng", "longitude", "x", "easting", "xlon", "xlong"}


def _import_csv(
    path: Path,
    layer_id: int,
    db: sqlite3.Connection,
    source_srid: int,
    lat_field: str | None,
    lon_field: str | None,
) -> ImportResult:
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return ImportResult(features_imported=0, features_failed=0, errors=["CSV has no data rows"], bbox=None)

    headers = list(rows[0].keys())

    # Auto-detect lat/lon columns
    if not lat_field:
        for h in headers:
            if h.lower() in _LAT_NAMES:
                lat_field = h
                break
    if not lon_field:
        for h in headers:
            if h.lower() in _LON_NAMES:
                lon_field = h
                break

    if not lat_field or not lon_field:
        raise ValueError(
            f"Cannot detect lat/lon columns. Found: {headers}. "
            "Specify lat_field and lon_field explicitly."
        )

    transformer = make_transformer(source_srid)
    records, errors, sample_props = [], [], []

    for i, row in enumerate(rows):
        try:
            lat = float(row[lat_field])
            lon = float(row[lon_field])
            geom = shapely.geometry.Point(lon, lat)
            if transformer:
                geom = reproject_geom(geom, transformer)
            props = {k: v for k, v in row.items() if k not in (lat_field, lon_field)}
            # Attempt numeric coercion
            props = _coerce_types(props)
            rec = _make_record(layer_id, geom, props)
            records.append(rec)
            if len(sample_props) < 100:
                sample_props.append(props)
        except Exception as e:
            errors.append(f"Row {i + 1}: {e}")

    imported, failed, batch_errors = _batch_insert(db, records)
    errors.extend(batch_errors)
    _update_attribute_schema(layer_id, db, sample_props)
    bbox = _compute_bbox(records)
    return ImportResult(features_imported=imported, features_failed=failed, errors=errors, bbox=bbox)


def _coerce_types(props: dict[str, str]) -> dict[str, Any]:
    result = {}
    for k, v in props.items():
        if v is None or v == "":
            result[k] = None
            continue
        try:
            result[k] = int(v)
            continue
        except (ValueError, TypeError):
            pass
        try:
            result[k] = float(v)
            continue
        except (ValueError, TypeError):
            pass
        result[k] = v
    return result


# ── Shared helpers ────────────────────────────────────────────────────────────

def _make_record(
    layer_id: int,
    geom: BaseGeometry,
    props: dict[str, Any],
    fid: Any = None,
) -> dict[str, Any]:
    wkb = geom_to_wkb(geom)
    minx, miny, maxx, maxy = bbox_from_geom(geom)
    return {
        "layer_id": layer_id,
        "fid": str(fid) if fid is not None else str(uuid.uuid4()),
        "geometry": wkb,
        "properties": json.dumps(props, default=str),
        "bbox_minx": minx,
        "bbox_miny": miny,
        "bbox_maxx": maxx,
        "bbox_maxy": maxy,
    }


def _batch_insert(
    db: sqlite3.Connection,
    records: list[dict[str, Any]],
    chunk_size: int = 500,
) -> tuple[int, int, list[str]]:
    imported, failed, errors = 0, 0, []
    sql = """
        INSERT OR IGNORE INTO features
            (layer_id, fid, geometry, properties, bbox_minx, bbox_miny, bbox_maxx, bbox_maxy)
        VALUES
            (:layer_id, :fid, :geometry, :properties, :bbox_minx, :bbox_miny, :bbox_maxx, :bbox_maxy)
    """
    for i in range(0, len(records), chunk_size):
        chunk = records[i : i + chunk_size]
        try:
            with db:
                db.executemany(sql, chunk)
            imported += len(chunk)
        except Exception as e:
            errors.append(f"Batch insert error (chunk {i // chunk_size}): {e}")
            failed += len(chunk)
    return imported, failed, errors


def _update_layer_stats(layer_id: int, db: sqlite3.Connection) -> None:
    row = db.execute(
        """SELECT COUNT(*) as cnt,
                  MIN(bbox_minx) as minx, MIN(bbox_miny) as miny,
                  MAX(bbox_maxx) as maxx, MAX(bbox_maxy) as maxy,
                  geometry
           FROM features WHERE layer_id = ? AND geometry IS NOT NULL""",
        (layer_id,),
    ).fetchone()

    geom_type = ""
    if row and row["geometry"]:
        try:
            import shapely.wkb
            g = shapely.wkb.loads(bytes(row["geometry"]))
            geom_type = g.geom_type
        except Exception:
            pass

    with db:
        db.execute(
            """UPDATE layers SET
                feature_count = ?,
                bbox_minx = ?, bbox_miny = ?, bbox_maxx = ?, bbox_maxy = ?,
                geometry_type = CASE WHEN geometry_type = '' THEN ? ELSE geometry_type END,
                updated_at = datetime('now')
               WHERE id = ?""",
            (
                row["cnt"] if row else 0,
                row["minx"], row["miny"], row["maxx"], row["maxy"],
                geom_type,
                layer_id,
            ),
        )


def _update_attribute_schema(
    layer_id: int, db: sqlite3.Connection, sample_props: list[dict]
) -> None:
    schema = infer_schema(sample_props)
    if schema:
        with db:
            db.execute(
                "UPDATE layers SET attribute_schema = ? WHERE id = ?",
                (json.dumps(schema), layer_id),
            )


def _compute_bbox(records: list[dict]) -> list[float] | None:
    vals = [(r["bbox_minx"], r["bbox_miny"], r["bbox_maxx"], r["bbox_maxy"]) for r in records if r.get("bbox_minx") is not None]
    if not vals:
        return None
    minx = min(v[0] for v in vals)
    miny = min(v[1] for v in vals)
    maxx = max(v[2] for v in vals)
    maxy = max(v[3] for v in vals)
    return [minx, miny, maxx, maxy]
