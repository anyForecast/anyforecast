import uvicorn
from fastapi import FastAPI

from .routers import registry


class WebApp:
    """FastAPI wrapper.
    """

    def __init__(self):
        self.app = FastAPI()

        for _, router in registry.get_routers().items():
            self.app.include_router(router)

    def start(self):
        """Runs FastAPI by calling uvicorn.
        """
        uvicorn.run(self.app, host="0.0.0.0", port=8000)


webapp = WebApp()
