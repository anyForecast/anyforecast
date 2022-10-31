============
Forecast API
============

Microservice API for providing forecasting services.


Getting Started
---------------
Assuming that you have Docker installed on your machine,

.. code-block:: sh

    $ git clone --recurse-submodules https://github.com/ramonAV98/forecast_api.git
    $ cd forecast_api
    $ docker-compose build

.. note::
    Depending on your git version, the way of including the repository
    submodules may vary. The following stackoverflow discussion addresses this
    issue very well: https://stackoverflow.com/questions/3796927/how-do-i-git-clone-a-repo-including-its-submodules


After the building process is finished, start an interactive bash
session using the forecast_api container,

.. code-block:: sh

    $ docker exec -it forecast_api bash


Inside the containers interactive session, navigate to the `forecast_api`
directory and start the FastAPI server. After the server is up, you can visit
the API docs located at localhost:80/docs using any browser.

.. code-block:: sh

    $ cd home/worker/forecast_api
    $ python -m uvicorn forecast_api.main:app --host=0.0.0.0 --port=80


Next, for the Celery server, start a second interactive bash session and do

.. code-block:: sh

    $ cd home/worker/forecast_api
    $ celery -A forecast_api.celery_app worker --loglevel=INFO



















