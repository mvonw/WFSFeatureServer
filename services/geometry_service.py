"""
Geometry utilities: WKB encode/decode, bbox extraction, GML 3.2 serialization,
CRS reprojection via pyproj.
"""
from __future__ import annotations

import json
from typing import Any

import shapely.geometry
import shapely.ops
import shapely.wkb
from shapely.geometry.base import BaseGeometry
from pyproj import Transformer, CRS


# ── CRS / reprojection ────────────────────────────────────────────────────────

def make_transformer(from_srid: int, to_srid: int = 4326) -> Transformer | None:
    """Return a pyproj Transformer or None if source is already WGS84."""
    if from_srid == to_srid:
        return None
    src = CRS.from_epsg(from_srid)
    dst = CRS.from_epsg(to_srid)
    return Transformer.from_crs(src, dst, always_xy=True)


def reproject_geom(geom: BaseGeometry, transformer: Transformer) -> BaseGeometry:
    """Reproject a Shapely geometry using the given transformer."""
    return shapely.ops.transform(transformer.transform, geom)


# ── WKB / bbox helpers ────────────────────────────────────────────────────────

def geom_to_wkb(geom: BaseGeometry) -> bytes:
    return shapely.wkb.dumps(geom, include_srid=False)


def wkb_to_geom(wkb: bytes) -> BaseGeometry:
    return shapely.wkb.loads(wkb)


def bbox_from_geom(geom: BaseGeometry) -> tuple[float, float, float, float]:
    """Return (minx, miny, maxx, maxy)."""
    return geom.bounds  # type: ignore[return-value]


# ── GeoJSON helper ────────────────────────────────────────────────────────────

def geom_to_geojson(geom: BaseGeometry) -> dict[str, Any]:
    return shapely.geometry.mapping(geom)


# ── GML 3.2 serialization ─────────────────────────────────────────────────────

def wkb_to_gml32(wkb: bytes, srid: int = 4326) -> str:
    geom = wkb_to_geom(wkb)
    srs = f"urn:ogc:def:crs:EPSG::{srid}"
    # EPSG:4326 axis order is lat,lon (Y,X) per OGC spec — swap X and Y in output.
    # All other CRS use X,Y as declared.
    swap = (srid == 4326)
    return _geom_to_gml(geom, srs, swap)


def _geom_to_gml(geom: BaseGeometry, srs: str, swap: bool = False) -> str:
    gtype = geom.geom_type
    if gtype == "Point":
        return _point_gml(geom, srs, swap)
    elif gtype == "LineString":
        return _linestring_gml(geom, srs, swap)
    elif gtype == "Polygon":
        return _polygon_gml(geom, srs, swap)
    elif gtype == "MultiPoint":
        return _multi_gml(geom, srs, swap, "MultiPoint", "pointMember", _point_gml)
    elif gtype == "MultiLineString":
        return _multi_gml(geom, srs, swap, "MultiCurve", "curveMember", _linestring_gml)
    elif gtype == "MultiPolygon":
        return _multi_gml(geom, srs, swap, "MultiSurface", "surfaceMember", _polygon_gml)
    elif gtype == "GeometryCollection":
        members = "".join(
            f"<gml:geometryMember>{_geom_to_gml(g, srs, swap)}</gml:geometryMember>"
            for g in geom.geoms
        )
        return f'<gml:MultiGeometry srsName="{srs}">{members}</gml:MultiGeometry>'
    raise ValueError(f"Unsupported geometry type: {gtype}")


def _coords_str(coords, swap: bool = False) -> str:
    if swap:
        return " ".join(f"{y} {x}" for x, y in coords)
    return " ".join(f"{x} {y}" for x, y in coords)


def _point_gml(geom: BaseGeometry, srs: str, swap: bool = False) -> str:
    pos = f"{geom.y} {geom.x}" if swap else f"{geom.x} {geom.y}"
    return (
        f'<gml:Point srsName="{srs}">'
        f"<gml:pos>{pos}</gml:pos>"
        f"</gml:Point>"
    )


def _linestring_gml(geom: BaseGeometry, srs: str, swap: bool = False) -> str:
    return (
        f'<gml:LineString srsName="{srs}">'
        f"<gml:posList>{_coords_str(geom.coords, swap)}</gml:posList>"
        f"</gml:LineString>"
    )


def _ring_gml(ring, swap: bool = False) -> str:
    return (
        f"<gml:LinearRing>"
        f"<gml:posList>{_coords_str(ring.coords, swap)}</gml:posList>"
        f"</gml:LinearRing>"
    )


def _polygon_gml(geom: BaseGeometry, srs: str, swap: bool = False) -> str:
    exterior = f"<gml:exterior>{_ring_gml(geom.exterior, swap)}</gml:exterior>"
    interiors = "".join(
        f"<gml:interior>{_ring_gml(r, swap)}</gml:interior>" for r in geom.interiors
    )
    return f'<gml:Polygon srsName="{srs}">{exterior}{interiors}</gml:Polygon>'


def _multi_gml(geom: BaseGeometry, srs: str, swap: bool, tag: str, member_tag: str, part_fn) -> str:
    members = "".join(
        f"<gml:{member_tag}>{part_fn(g, srs, swap)}</gml:{member_tag}>"
        for g in geom.geoms
    )
    return f'<gml:{tag} srsName="{srs}">{members}</gml:{tag}>'


# ── Attribute type inference ───────────────────────────────────────────────────

def infer_schema(sample_props: list[dict[str, Any]]) -> dict[str, str]:
    """
    Infer attribute types from a sample of property dicts.
    Returns {"field_name": "String"|"Integer"|"Real"|"Date"}.
    """
    if not sample_props:
        return {}

    fields: dict[str, set[str]] = {}
    for props in sample_props:
        for k, v in props.items():
            if k not in fields:
                fields[k] = set()
            fields[k].add(_value_type(v))

    result: dict[str, str] = {}
    for k, types in fields.items():
        if types == {"Integer"}:
            result[k] = "Integer"
        elif types <= {"Integer", "Real"}:
            result[k] = "Real"
        else:
            result[k] = "String"
    return result


def _value_type(v: Any) -> str:
    if isinstance(v, bool):
        return "String"
    if isinstance(v, int):
        return "Integer"
    if isinstance(v, float):
        return "Real"
    return "String"


