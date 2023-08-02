from typing import Annotated

from fastapi import Depends
from fastapi import FastAPI

from .routers import registry
from .settings import get_api_settings, APISettings


def start_app() -> FastAPI:
    """Application factory function.

    Returns a FastAPI app for us to use.

    Returns
    -------
    app : FastAPI
    """
    app = FastAPI()

    for _, router in registry.get_routers().items():
        app.include_router(router)

    return app


app = start_app()


@app.get("/info")
async def info(settings: Annotated[APISettings, Depends(get_api_settings)]):
    """Application general information.
    """
    return settings.dict()
