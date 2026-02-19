from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    admin_user: str = "admin"
    admin_pass: str = "changeme"
    db_path: str = "data/geofeatures.db"
    uploads_dir: str = "uploads"
    max_features_per_request: int = 10000
    service_title: str = "GeoFeatureService"
    service_abstract: str = "Lightweight WFS 2.0.0 feature server"
    service_url: str = "http://localhost:8000/wfs"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
