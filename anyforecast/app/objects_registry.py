from __future__ import annotations

from typing import Any, Dict


def create_registry() -> ObjectsRegistry:
    """Factory of :class:`ObjectRegistry` instances.
    Returns
    -------
    registry : ObjectsRegistry
    """
    return ObjectsRegistry()


class ObjectsRegistry:
    """Generic objects registry."""

    def __init__(self):
        self._registry: Dict[str, Any] = {}

    def get(self, name: str) -> Any:
        """Retrieves objective function by its name.
        Parameters
        ----------
        name : str
            Objective function name.
        """
        if name not in self._registry:
            raise ValueError(f"Object '{name}' does not exist.")
        return self._registry[name]

    def get_registry(self) -> Dict[str, Any]:
        """Returns registered objects."""
        return self._registry

    def put(self, name, obj: Any) -> None:
        """Registers new object.

        Parameters
        ----------
        name : str
            Object identifier name. Use this name to retrieve the object through
            method :meth:`get`.

        obj : Any
            Object to store.
        """
        self._registry[name] = obj
