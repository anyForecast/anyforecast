from .oauth2 import router as oauth2_router
from .grafana import router as grafana_router

ROUTERS = [
    oauth2_router,
    grafana_router
]

__all__ = [
    'ROUTERS'
]
