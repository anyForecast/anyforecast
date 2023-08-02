from abc import ABC, abstractmethod
from typing import (
    Dict
)


class Task(ABC):
    """Base class for anyForecast tasks.

    .. note::
        This class should not be used directly. Use derived classes instead.
    """

    def __init__(self, name, **kwargs):
        self.name = name
        self.kwargs = kwargs

    def get_kwargs(self, prefix: str) -> Dict:
        """Extracts kwargs containing the given prefix.

        Parameters
        ----------
        prefix : str
            Kwargs with this prefix are returned.

        Returns
        -------
        kwargs : dict
            Kwargs containing the given prefix.
        """
        if not prefix.endswith('__'):
            prefix += '__'

        return {key[len(prefix):]: val for key, val
                in self.kwargs.items()
                if key.startswith(prefix)}

    @abstractmethod
    def run(self):
        """Holds the actual task logic.
        """
        pass




