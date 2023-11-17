from typing import Annotated

import uvicorn
from fastapi import Depends, FastAPI

from anyforecast.settings import AppPublicInfo, get_public_info

from .routers import login, models, users


def create_fastapi_app() -> FastAPI:
    app = FastAPI()
    app.include_router(users.router)
    app.include_router(login.router)
    app.include_router(models.router)

    return app


class AnyForecastWebApp:
    """AnyForecast webapp."""

    def __init__(self):
        self.fastapi = create_fastapi_app()

    def run_server(
        self, host: str = "0.0.0.0", port: int = 80, reload: bool = False
    ):
        """Runs FastAPI by calling uvicorn."""
        uvicorn.run(self.fastapi, host=host, port=port, reload=reload)


webapp = AnyForecastWebApp()


@webapp.fastapi.get("/info")
async def info(info: Annotated[AppPublicInfo, Depends(get_public_info)]):
    """Application general information."""
    return info.model_dump()
