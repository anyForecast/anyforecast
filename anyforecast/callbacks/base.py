from typing import Any


class Callback:
    """Base class for callbacks.

    All custom callbacks should inherit from this class. The subclass
    may override any of the ``on_...`` methods.

    . note::
        This class should not be used directly. Use derived classes instead.
    """

    def on_begin(self, task_id: str) -> None:
        """Called at the beginning of running."""

    def on_success(self, retval: Any, task_id: str) -> None:
        """Called when task completes successfully."""

    def on_failure(self, exc: Any, task_id: str) -> None:
        """Called when task fails."""
