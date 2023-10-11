from .task import TasksFactory


@TasksFactory.register()
def add(x, y):
    return x + y
