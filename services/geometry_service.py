"""
Geometry utilities: WKB encode/decode, bbox extraction, GML 3.2 serialization
and parsing, CRS reprojection via pyproj.
"""
from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from typing import Any

import shapely.geometry
import shapely.ops
import shapely.wkb
from shapely.geometry.base import BaseGeometry
from pyproj import Transformer, CRS

_GML_NS = "http://www.opengis.net/gml/3.2"


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


# ── GML 3.2 parsing (inverse of serialization) ───────────────────────────────

def _gml_tag(local: str) -> str:
    """Return fully-qualified GML 3.2 tag."""
    return f"{{{_GML_NS}}}{local}"


def _parse_srs(elem: ET.Element) -> tuple[int, bool]:
    """Extract SRID and whether axis swap is needed from srsName attribute.
    Returns (srid, swap). Defaults to (4326, True) if no srsName found."""
    srs = elem.get("srsName", "")
    m = re.search(r"EPSG::(\d+)", srs)
    srid = int(m.group(1)) if m else 4326
    swap = (srid == 4326)
    return srid, swap


def _parse_pos(text: str, swap: bool) -> tuple[float, float]:
    """Parse a gml:pos string into (x, y), handling axis swap."""
    parts = text.strip().split()
    a, b = float(parts[0]), float(parts[1])
    return (b, a) if swap else (a, b)


def _parse_poslist(text: str, swap: bool) -> list[tuple[float, float]]:
    """Parse a gml:posList string into a list of (x, y) tuples."""
    parts = text.strip().split()
    coords = []
    for i in range(0, len(parts), 2):
        a, b = float(parts[i]), float(parts[i + 1])
        coords.append((b, a) if swap else (a, b))
    return coords


def _find_text(elem: ET.Element, local: str) -> str:
    """Find a GML child element and return its text content."""
    child = elem.find(_gml_tag(local))
    if child is None or child.text is None:
        raise ValueError(f"Missing <gml:{local}> element")
    return child.text


def _parse_point(elem: ET.Element, swap: bool) -> BaseGeometry:
    pos_text = _find_text(elem, "pos")
    x, y = _parse_pos(pos_text, swap)
    return shapely.geometry.Point(x, y)


def _parse_linestring(elem: ET.Element, swap: bool) -> BaseGeometry:
    poslist_text = _find_text(elem, "posList")
    coords = _parse_poslist(poslist_text, swap)
    return shapely.geometry.LineString(coords)


def _parse_linearring(elem: ET.Element, swap: bool) -> list[tuple[float, float]]:
    poslist_text = _find_text(elem, "posList")
    return _parse_poslist(poslist_text, swap)


def _parse_polygon(elem: ET.Element, swap: bool) -> BaseGeometry:
    exterior_elem = elem.find(_gml_tag("exterior"))
    if exterior_elem is None:
        raise ValueError("Polygon missing <gml:exterior>")
    ring_elem = exterior_elem.find(_gml_tag("LinearRing"))
    if ring_elem is None:
        raise ValueError("Polygon exterior missing <gml:LinearRing>")
    exterior = _parse_linearring(ring_elem, swap)

    holes = []
    for interior_elem in elem.findall(_gml_tag("interior")):
        ring = interior_elem.find(_gml_tag("LinearRing"))
        if ring is not None:
            holes.append(_parse_linearring(ring, swap))

    return shapely.geometry.Polygon(exterior, holes)


def _parse_multi(elem: ET.Element, swap: bool, member_tag: str, part_fn) -> list:
    parts = []
    for member in elem.findall(_gml_tag(member_tag)):
        child = list(member)
        if child:
            parts.append(part_fn(child[0], swap))
    return parts


def _parse_gml_element(elem: ET.Element, swap: bool) -> BaseGeometry:
    """Parse a single GML geometry element to a Shapely geometry."""
    local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag

    if local == "Point":
        return _parse_point(elem, swap)
    elif local == "LineString":
        return _parse_linestring(elem, swap)
    elif local == "Polygon":
        return _parse_polygon(elem, swap)
    elif local == "MultiPoint":
        points = _parse_multi(elem, swap, "pointMember", _parse_point)
        return shapely.geometry.MultiPoint(points)
    elif local == "MultiCurve":
        lines = _parse_multi(elem, swap, "curveMember", _parse_linestring)
        return shapely.geometry.MultiLineString(lines)
    elif local == "MultiSurface":
        polys = _parse_multi(elem, swap, "surfaceMember", _parse_polygon)
        return shapely.geometry.MultiPolygon(polys)
    elif local == "MultiGeometry":
        geoms = _parse_multi(elem, swap, "geometryMember", _parse_gml_element)
        return shapely.geometry.GeometryCollection(geoms)
    else:
        raise ValueError(f"Unsupported GML geometry type: {local}")


def gml32_to_geom(elem: ET.Element) -> tuple[BaseGeometry, int]:
    """Parse a GML 3.2 geometry element into a Shapely geometry.

    Returns (geometry, srid) where srid is extracted from srsName.
    EPSG:4326 axis order (lat,lon) is automatically swapped to (lon,lat).
    """
    srid, swap = _parse_srs(elem)
    geom = _parse_gml_element(elem, swap)
    return geom, srid


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


