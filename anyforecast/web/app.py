import uvicorn
from fastapi import FastAPI

from anyforecast.models.session import check_db

from .routers import login, tasks, users


def create_fastapi_app() -> FastAPI:
    app = FastAPI()
    app.include_router(users.router)
    app.include_router(login.router)
    app.include_router(tasks.router)

    return app


class AnyForecastWebApp:
    """AnyForecast webapp."""

    def __init__(self):
        check_db("create")
        self.fastapi = create_fastapi_app()

    def run_server(self, host="0.0.0.0", port=80):
        """Runs FastAPI by calling uvicorn."""
        uvicorn.run(self.fastapi, host=host, port=port)


webapp = AnyForecastWebApp()
