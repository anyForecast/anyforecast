from . import login, users

submodules = (
    login,
    users
)


class RoutersRegistry:
    def __init__(self):
        self._registry = {}

    def add_router(self, name, router) -> None:
        self._registry[name] = router

    def get_routers(self) -> dict:
        return self._registry

    @classmethod
    def from_submodules(cls, submodules: tuple):
        registry = cls()

        for mod in submodules:
            registry.add_router(mod.__name__, mod.router)

        return registry


registry = RoutersRegistry.from_submodules(submodules)

__all__ = [
    'registry'
]
