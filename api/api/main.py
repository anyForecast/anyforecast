import typing

import orjson
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .routes import ROUTERS


class ORJSONResponse(JSONResponse):
    media_type = "application/json"

    def render(self, content: typing.Any) -> bytes:
        return orjson.dumps(content)


app = FastAPI(default_response_class=ORJSONResponse)

for router in ROUTERS:
    app.include_router(router)
