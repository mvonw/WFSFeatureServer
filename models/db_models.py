from __future__ import annotations
import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Layer:
    id: int
    name: str
    title: str
    description: str
    geometry_type: str
    srid: int
    bbox_minx: float | None
    bbox_miny: float | None
    bbox_maxx: float | None
    bbox_maxy: float | None
    feature_count: int
    attribute_schema: dict[str, str]
    created_at: str
    updated_at: str

    @classmethod
    def from_row(cls, row) -> "Layer":
        return cls(
            id=row["id"],
            name=row["name"],
            title=row["title"],
            description=row["description"],
            geometry_type=row["geometry_type"],
            srid=row["srid"],
            bbox_minx=row["bbox_minx"],
            bbox_miny=row["bbox_miny"],
            bbox_maxx=row["bbox_maxx"],
            bbox_maxy=row["bbox_maxy"],
            feature_count=row["feature_count"],
            attribute_schema=json.loads(row["attribute_schema"] or "{}"),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @property
    def has_bbox(self) -> bool:
        return all(v is not None for v in [self.bbox_minx, self.bbox_miny, self.bbox_maxx, self.bbox_maxy])


@dataclass
class Feature:
    id: int
    layer_id: int
    fid: str
    geometry: bytes | None
    properties: dict[str, Any]
    bbox_minx: float | None
    bbox_miny: float | None
    bbox_maxx: float | None
    bbox_maxy: float | None

    @classmethod
    def from_row(cls, row) -> "Feature":
        return cls(
            id=row["id"],
            layer_id=row["layer_id"],
            fid=row["fid"],
            geometry=row["geometry"],
            properties=json.loads(row["properties"] or "{}"),
            bbox_minx=row["bbox_minx"],
            bbox_miny=row["bbox_miny"],
            bbox_maxx=row["bbox_maxx"],
            bbox_maxy=row["bbox_maxy"],
        )


@dataclass
class SymbologyRule:
    id: int
    layer_id: int
    rule_order: int
    label: str
    filter_field: str | None
    filter_operator: str
    filter_value: str | None
    fill_color: str
    fill_opacity: float
    stroke_color: str
    stroke_width: float
    point_radius: float
    is_default: bool

    @classmethod
    def from_row(cls, row) -> "SymbologyRule":
        return cls(
            id=row["id"],
            layer_id=row["layer_id"],
            rule_order=row["rule_order"],
            label=row["label"],
            filter_field=row["filter_field"],
            filter_operator=row["filter_operator"],
            filter_value=row["filter_value"],
            fill_color=row["fill_color"],
            fill_opacity=row["fill_opacity"],
            stroke_color=row["stroke_color"],
            stroke_width=row["stroke_width"],
            point_radius=row["point_radius"],
            is_default=bool(row["is_default"]),
        )
