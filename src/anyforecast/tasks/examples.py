from anyforecast.tasks import TasksFactory


@TasksFactory.register()
def add(x, y):
    return x + y
