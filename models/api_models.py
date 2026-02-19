from __future__ import annotations
from typing import Any, Literal, Optional
from pydantic import BaseModel, Field


# ── Layer ────────────────────────────────────────────────────────────────────

class LayerCreate(BaseModel):
    name: str = Field(..., pattern=r"^[A-Za-z0-9_\-]+$", description="Machine-safe WFS TypeName")
    title: str = ""
    description: str = ""


class LayerUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None


class LayerResponse(BaseModel):
    id: int
    name: str
    title: str
    description: str
    geometry_type: str
    srid: int
    bbox: Optional[list[float]] = None   # [minx, miny, maxx, maxy] or None
    feature_count: int
    attribute_schema: dict[str, str]
    created_at: str
    updated_at: str


# ── Import ────────────────────────────────────────────────────────────────────

class ImportResult(BaseModel):
    features_imported: int
    features_failed: int
    errors: list[str]
    bbox: Optional[list[float]] = None


# ── Symbology ────────────────────────────────────────────────────────────────

FilterOperator = Literal["eq", "neq", "gt", "gte", "lt", "lte", "contains", "in", "is_null"]


class SymbologyRuleCreate(BaseModel):
    rule_order: int = 0
    label: str = ""
    filter_field: Optional[str] = None
    filter_operator: FilterOperator = "eq"
    filter_value: Optional[str] = None
    fill_color: str = Field(default="#3388ff", pattern=r"^#[0-9A-Fa-f]{6}$")
    fill_opacity: float = Field(default=0.6, ge=0.0, le=1.0)
    stroke_color: str = Field(default="#ffffff", pattern=r"^#[0-9A-Fa-f]{6}$")
    stroke_width: float = Field(default=1.5, ge=0.0, le=50.0)
    point_radius: float = Field(default=6.0, ge=1.0, le=100.0)
    is_default: bool = False


class SymbologyRuleUpdate(SymbologyRuleCreate):
    pass


class SymbologyRuleResponse(SymbologyRuleCreate):
    id: int
    layer_id: int


class SymbologyReorderRequest(BaseModel):
    order: list[int]   # ordered list of rule IDs
