from .tasks_registry import task


@task
def add(x, y):
    return x + y
