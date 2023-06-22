Containerized machine learning end-to-end lifecycle system for time series 
forecasting.  A manifest file (i.e., Dockerfile) is provided for building all 
the required services.
 
Services
--------

| Name          | Description                             | UI                      |
|---------------|-----------------------------------------|-------------------------|
| `mlflow`      | Experiment tracking and model registry. | http://localhost:5000   |
| `jupyter lab` | Data analysis / experimentation.        | http://localhost:8888   |                  |
| `minio`       | Objects storage.                        | http://localhost:9000   |
| `api`         | Hosts the actual API code.              | http://localhost:80/docs |
| `worker`      | Tasks executor.                         | NA                      |
| `postgres`    | Backend mlflow storage.                 | NA                      |

> **Note**
> The default ports can be changed through the .env file.



Run on Amazon ECS
-----------------



Run locally
-------------------
1. Build the services with `docker-compose build`.
2. Run the services with `docker-compose up`.

    

