============
Forecast API
============

Microservice API for providing forecasting services.

A docker-compose.yml file is provided for building all the required services, which include

* mlflow, default_port=5001
    Used for tracking and serving machine learning models.

* jupyterlab, default_port=7777
    Used for code experimentation, developing, data analysis and whatever the user wants.  

* minio, default_port=9000
    Used for objects storage. By default, trained machine learning models are stored here inside a bucket named *pipelines*.

* postgres, default_port=5432
    By default, it is used for keeping mlflow results. However, the users can extend its purpose to their needs.

* rabbitmq, default_port=5672
    Used as message broker and backend for Celery.

* forecast_api, default_port=80
    This container hosts the actual API code, which is built using FastAPI and Celery as task queue manager. 


.. note::
    The default ports can be changed through the .env file.



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


.. warning::
    Currently, the bash session starts user as ``root`` which is discouraged and
    must be avoided. This is a temporal behaviour needed by mlflow for building docker images
    from inside the container. 


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



















