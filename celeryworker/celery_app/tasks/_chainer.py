from celery import chain


class TaskChainer:
    def __init__(self):
        self._signatures = []

    def add_task(self, task, **kwargs):
        task_signature = task.s(**kwargs)
        self._signatures.append(task_signature)

    def get_signatures(self):
        return self._signatures

    def make_chain(self):
        return chain(*self._signatures)
