"""
OGC WFS 2.0.0 endpoint.
Handles KVP (Key-Value Pair) GET requests for:
  GetCapabilities, DescribeFeatureType, GetFeature
"""
from __future__ import annotations

import sqlite3
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse, Response

from config import settings
from database import get_db
from services import wfs_service

router = APIRouter()

_GML_CONTENT_TYPE = "application/gml+xml; version=3.2; charset=UTF-8"
_XML_CONTENT_TYPE = "application/xml; charset=UTF-8"


@router.get("/wfs")
@router.post("/wfs")
async def wfs_endpoint(
    SERVICE: Optional[str] = Query(default=None),
    service: Optional[str] = Query(default=None),
    VERSION: Optional[str] = Query(default=None),
    version: Optional[str] = Query(default=None),
    REQUEST: Optional[str] = Query(default=None),
    request: Optional[str] = Query(default=None),
    TYPENAMES: Optional[str] = Query(default=None),
    TypeNames: Optional[str] = Query(default=None),
    typenames: Optional[str] = Query(default=None),
    TYPENAME: Optional[str] = Query(default=None),
    TypeName: Optional[str] = Query(default=None),
    BBOX: Optional[str] = Query(default=None),
    bbox: Optional[str] = Query(default=None),
    COUNT: Optional[int] = Query(default=None),
    count: Optional[int] = Query(default=None),
    STARTINDEX: int = Query(default=0),
    startindex: int = Query(default=0),
    OUTPUTFORMAT: Optional[str] = Query(default=None),
    outputFormat: Optional[str] = Query(default=None),
    outputformat: Optional[str] = Query(default=None),
    db: sqlite3.Connection = Depends(get_db),
):
    # Normalise case-insensitive KVP params
    req = (REQUEST or request or "").strip()
    type_names = TYPENAMES or TypeNames or typenames or TYPENAME or TypeName
    bbox_str = BBOX or bbox
    req_count = COUNT or count
    req_startindex = STARTINDEX or startindex
    output_fmt = OUTPUTFORMAT or outputFormat or outputformat or ""

    req_upper = req.upper()

    if req_upper == "GETCAPABILITIES" or req == "":
        xml = wfs_service.build_capabilities(db)
        return Response(content=xml, media_type=_XML_CONTENT_TYPE)

    elif req_upper == "DESCRIBEFEATURETYPE":
        xml = wfs_service.build_describe(type_names, db)
        return Response(content=xml, media_type=_XML_CONTENT_TYPE)

    elif req_upper == "GETFEATURE":
        if not type_names:
            raise HTTPException(status_code=400, detail="TYPENAMES parameter is required for GetFeature")

        parsed_bbox = _parse_bbox(bbox_str) if bbox_str else None

        fmt_lower = output_fmt.lower()
        if "json" in fmt_lower or "geojson" in fmt_lower:
            result = wfs_service.build_get_feature_geojson(
                typenames=type_names,
                db=db,
                bbox=parsed_bbox,
                count=req_count,
                startindex=req_startindex,
                max_features=settings.max_features_per_request,
            )
            return JSONResponse(result)
        else:
            gml = wfs_service.build_get_feature_gml(
                typenames=type_names,
                db=db,
                bbox=parsed_bbox,
                count=req_count,
                startindex=req_startindex,
                max_features=settings.max_features_per_request,
            )
            return Response(content=gml, media_type=_GML_CONTENT_TYPE)

    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown REQUEST: '{req}'. Supported: GetCapabilities, DescribeFeatureType, GetFeature",
        )


def _parse_bbox(bbox_str: str) -> tuple[float, float, float, float]:
    """Parse 'minx,miny,maxx,maxy[,CRS]' string."""
    parts = bbox_str.split(",")
    if len(parts) < 4:
        raise HTTPException(status_code=400, detail=f"Invalid BBOX: '{bbox_str}'")
    try:
        return float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3])
    except ValueError:
        raise HTTPException(status_code=400, detail=f"BBOX values must be numeric: '{bbox_str}'")
