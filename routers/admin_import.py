import shutil
import sqlite3
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from config import settings
from database import get_db
from models.api_models import ImportResult
from services.import_service import import_file

router = APIRouter()


@router.post("/layers/{layer_id}/import", response_model=ImportResult)
async def import_layer_file(
    layer_id: int,
    file: UploadFile = File(...),
    srid: int = Form(default=4326),
    lat_field: str | None = Form(default=None),
    lon_field: str | None = Form(default=None),
    replace_existing: bool = Form(default=False),
    db: sqlite3.Connection = Depends(get_db),
):
    # Verify layer exists
    row = db.execute("SELECT id FROM layers WHERE id = ?", (layer_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Layer {layer_id} not found")

    # Validate extension
    filename = file.filename or "upload"
    suffix = Path(filename).suffix.lower()
    allowed = {".geojson", ".json", ".zip", ".gpkg", ".csv"}
    if suffix not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{suffix}'. Allowed: {', '.join(sorted(allowed))}",
        )

    # Save upload to temp file
    uploads_dir = Path(settings.uploads_dir)
    uploads_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = uploads_dir / f"layer_{layer_id}_{filename}"

    try:
        with open(tmp_path, "wb") as dst:
            shutil.copyfileobj(file.file, dst)

        result = import_file(
            file_path=tmp_path,
            layer_id=layer_id,
            db=db,
            source_srid=srid,
            lat_field=lat_field or None,
            lon_field=lon_field or None,
            replace_existing=replace_existing,
        )
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))
    finally:
        tmp_path.unlink(missing_ok=True)

    return result
