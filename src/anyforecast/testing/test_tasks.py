from anyforecast.registry import Registry

registry = Registry()


@registry()
def add(x, y):
    return x + y



