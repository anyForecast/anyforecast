import sys
from os.path import dirname, abspath

from celery import Celery

from . import celeryconfig

parent_parent_dir = dirname(dirname(abspath(__file__)))
sys.path.append(parent_parent_dir)

app = Celery('celery_app')

app.config_from_object(celeryconfig)
app.conf.update(CELERY_RESULT_BACKEND="rpc://")

if __name__ == '__main__':
    app.start()
