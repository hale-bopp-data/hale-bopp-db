"""DB-HALE-BOPP — Schema governance engine for PostgreSQL."""

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import pathlib

from app.api.routes import router
from app.version import __version__

app = FastAPI(
    title="hale-bopp-db",
    description="Deterministic schema diff, planning, deploy, drift detection, and dictionary tooling for PostgreSQL.",
    version=__version__,
)

app.include_router(router)

@app.get("/console", response_class=HTMLResponse)
def get_console():
    """Interactive Graphical Console for hale-bopp-db, inspired by Valentino Cockpit."""
    console_path = pathlib.Path(__file__).parent / "console.html"
    return console_path.read_text(encoding="utf-8")
