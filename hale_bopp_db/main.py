"""DB-HALE-BOPP — Schema governance engine for PostgreSQL."""

from fastapi import FastAPI

from hale_bopp_db.api.routes import router

app = FastAPI(
    title="DB-HALE-BOPP",
    description="Deterministic schema diff, deploy, and drift detection engine.",
    version="0.1.0",
)

app.include_router(router)
