"""
OGC WFS 2.0.0 endpoint.
Handles KVP (Key-Value Pair) GET/POST requests for:
  GetCapabilities, DescribeFeatureType, GetFeature, Transaction (WFS-T)
"""
from __future__ import annotations

import sqlite3
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

from config import settings
from database import get_db
from services import wfs_service, transaction_service

router = APIRouter()

_GML_CONTENT_TYPE = "application/gml+xml; version=3.2; charset=UTF-8"
_XML_CONTENT_TYPE = "application/xml; charset=UTF-8"


@router.get("/wfs")
@router.post("/wfs")
async def wfs_endpoint(
    raw_request: Request,
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

    # Handle XML POST body for Transaction requests
    if raw_request.method == "POST" and (req_upper == "TRANSACTION" or req_upper == ""):
        content_type = (raw_request.headers.get("content-type") or "").lower()
        if "xml" in content_type or req_upper == "TRANSACTION":
            body = await raw_request.body()
            if body and (req_upper == "TRANSACTION" or b"Transaction" in body):
                xml = transaction_service.execute_transaction(body, db)
                return Response(content=xml, media_type=_XML_CONTENT_TYPE)

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

    elif req_upper == "TRANSACTION":
        raise HTTPException(status_code=400, detail="Transaction requires XML POST body")

    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown REQUEST: '{req}'. Supported: GetCapabilities, DescribeFeatureType, GetFeature, Transaction",
        )


def _parse_bbox(bbox_str: str) -> tuple[float, float, float, float]:
    """Parse 'minx,miny,maxx,maxy[,CRS]' string.

    WFS 2.0.0: when the CRS suffix indicates EPSG:4326 (lat/lon axis order),
    the values arrive as minLat,minLon,maxLat,maxLon — swap to minx,miny,maxx,maxy.
    """
    parts = bbox_str.split(",")
    if len(parts) < 4:
        raise HTTPException(status_code=400, detail=f"Invalid BBOX: '{bbox_str}'")
    try:
        v0, v1, v2, v3 = float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3])
    except ValueError:
        raise HTTPException(status_code=400, detail=f"BBOX values must be numeric: '{bbox_str}'")

    # If a CRS is appended, check for EPSG:4326 axis swap (lat/lon → lon/lat)
    if len(parts) >= 5:
        crs = parts[4].strip()
        if "4326" in crs and ("EPSG" in crs or "CRS84" not in crs):
            # Values are lat,lon order — swap to lon,lat for internal use
            return v1, v0, v3, v2

    return v0, v1, v2, v3
