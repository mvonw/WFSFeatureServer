import base64
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

from config import settings
from database import init_db
from routers import wfs, admin_layers, admin_import, admin_symbology


# ── Basic Auth middleware ─────────────────────────────────────────────────────

class BasicAuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, username: str, password: str):
        super().__init__(app)
        self._token = base64.b64encode(f"{username}:{password}".encode()).decode()

    async def dispatch(self, request: Request, call_next):
        if request.url.path.startswith("/api/admin"):
            auth = request.headers.get("Authorization", "")
            if not auth.startswith("Basic ") or auth[6:] != self._token:
                return Response(
                    content="Unauthorized",
                    status_code=401,
                    headers={"WWW-Authenticate": 'Basic realm="GeoFeatureService Admin"'},
                )
        return await call_next(request)


# ── App lifespan ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    Path(settings.uploads_dir).mkdir(parents=True, exist_ok=True)
    yield


# ── App setup ─────────────────────────────────────────────────────────────────

app = FastAPI(
    title="GeoFeatureService",
    description="Lightweight WFS 2.0.0 feature server with admin UI",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(BasicAuthMiddleware, username=settings.admin_user, password=settings.admin_pass)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(wfs.router,              tags=["WFS"])
app.include_router(admin_layers.router,     prefix="/api/admin", tags=["Admin - Layers"])
app.include_router(admin_import.router,     prefix="/api/admin", tags=["Admin - Import"])
app.include_router(admin_symbology.router,  prefix="/api/admin", tags=["Admin - Symbology"])

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse("/static/admin.html")
