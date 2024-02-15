def gen_default_name(fun: callable) -> str:
    """Generates default name from the given function."""
    name = fun.__name__
    module_name = fun.__module__
    return ".".join([module_name, name])


class Registry(dict):
    """Generic objects registry."""

    def __init__(self, exception: Exception = KeyError):
        self.exception = exception

    def __missing__(self, key: str):
        self.raise_exception(key)

    def __call__(self, name: str | None = None) -> None:
        return self.register(name)

    def raise_exception(self, key: str):
        raise self.exception(key)

    def register(self, name: str | None = None) -> None:
        """Registers functions in the internal registry.

        Use it as decorator to register objects in the internal registry.

        Example
        -------
        registry = Registry()

        @registry()
            def foo():
                ...

        Parameters
        ----------
        name : str, default = None
            Name under which to store the function object.

        Returns
        -------
        inner : callable
            Actual decorator that registers objects to the internal registry
            under the given name.
        """

        def inner(fun: callable) -> None:
            name_ = name or gen_default_name(fun)
            self[name_] = fun

        return inner

    def unregister(self, name: str):
        """Unregisters a function by name.

        Parameters
        ----------
        name : str
            Name of the function object to unregister

        Raises
        ------
        KeyError
        """
        try:
            self.pop(name)
        except KeyError:
            self.raise_exception(name)
