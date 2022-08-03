from fastapi import FastAPI

from .routes import ROUTERS

app = FastAPI()
for router in ROUTERS:
    app.include_router(router)
