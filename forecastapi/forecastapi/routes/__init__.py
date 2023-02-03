from .oauth2 import router as oauth2_router

ROUTERS = [
    oauth2_router
]

__all__ = [
    'ROUTERS'
]
