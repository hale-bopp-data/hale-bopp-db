"""DB-HALE-BOPP — Schema governance engine for PostgreSQL."""

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import pathlib

from app.api.routes import router

app = FastAPI(
    title="DB-HALE-BOPP",
    description="Deterministic schema diff, deploy, and drift detection engine.",
    version="0.1.0",
)

app.include_router(router)

@app.get("/console", response_class=HTMLResponse)
def get_console():
    """Interactive Graphical Console for hale-bopp-db, inspired by Valentino Cockpit."""
    console_path = pathlib.Path(__file__).parent / "console.html"
    return console_path.read_text(encoding="utf-8")
