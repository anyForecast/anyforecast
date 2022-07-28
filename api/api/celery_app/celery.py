import sys
from os.path import dirname, abspath

from celery import Celery

parent_parent_dir = dirname(dirname(abspath(__file__)))
sys.path.append(parent_parent_dir)

app = Celery('celery_app')
app.config_from_object('celery_app.celeryconfig')

if __name__ == '__main__':
    app.start()
