from celery import Celery

from worker.tasks import celeryconfig

app = Celery('train')
app.config_from_object(celeryconfig)

if __name__ == '__main__':
    app.start()
