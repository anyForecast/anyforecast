from typing import Annotated

from fastapi import Depends, FastAPI

from anyforecast.settings import AppPublicInfo, get_public_info

from .routers import login, models, users


def create_fastapi_app() -> FastAPI:
    app = FastAPI()
    app.include_router(users.router)
    app.include_router(login.router)
    app.include_router(models.router)

    return app


app = create_fastapi_app()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/info")
async def info(info: Annotated[AppPublicInfo, Depends(get_public_info)]):
    """Application general information."""
    return info.model_dump()
