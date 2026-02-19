import sqlite3
from pathlib import Path
from config import settings


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(settings.db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    Path(settings.db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = get_db()
    with conn:
        conn.executescript("""
CREATE TABLE IF NOT EXISTS layers (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT    NOT NULL UNIQUE,
    title            TEXT    NOT NULL DEFAULT '',
    description      TEXT    NOT NULL DEFAULT '',
    geometry_type    TEXT    NOT NULL DEFAULT '',
    srid             INTEGER NOT NULL DEFAULT 4326,
    bbox_minx        REAL,
    bbox_miny        REAL,
    bbox_maxx        REAL,
    bbox_maxy        REAL,
    feature_count    INTEGER NOT NULL DEFAULT 0,
    attribute_schema TEXT    NOT NULL DEFAULT '{}',
    created_at       TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at       TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS features (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    layer_id   INTEGER NOT NULL REFERENCES layers(id) ON DELETE CASCADE,
    fid        TEXT    NOT NULL,
    geometry   BLOB,
    properties TEXT    NOT NULL DEFAULT '{}',
    bbox_minx  REAL,
    bbox_miny  REAL,
    bbox_maxx  REAL,
    bbox_maxy  REAL,
    UNIQUE(layer_id, fid)
);

CREATE INDEX IF NOT EXISTS idx_features_layer
    ON features(layer_id);
CREATE INDEX IF NOT EXISTS idx_features_bbox
    ON features(layer_id, bbox_minx, bbox_miny, bbox_maxx, bbox_maxy);

CREATE TABLE IF NOT EXISTS symbology_rules (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    layer_id        INTEGER NOT NULL REFERENCES layers(id) ON DELETE CASCADE,
    rule_order      INTEGER NOT NULL DEFAULT 0,
    label           TEXT    NOT NULL DEFAULT '',
    filter_field    TEXT,
    filter_operator TEXT    NOT NULL DEFAULT 'eq',
    filter_value    TEXT,
    fill_color      TEXT    NOT NULL DEFAULT '#3388ff',
    fill_opacity    REAL    NOT NULL DEFAULT 0.6,
    stroke_color    TEXT    NOT NULL DEFAULT '#ffffff',
    stroke_width    REAL    NOT NULL DEFAULT 1.5,
    point_radius    REAL    NOT NULL DEFAULT 6.0,
    is_default      INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_rules_layer
    ON symbology_rules(layer_id, rule_order);
        """)
    conn.close()
