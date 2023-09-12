from .decorators import task


@task
def add(x, y):
    return x + y
