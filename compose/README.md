# anyforecast on docker compose

This directory contains the needed configuration files for running
anyforecast on docker compose.

A ``compose-core.yml`` file is provided for building the required services,
which include

| Name     | Description                          | GUI | Port |
|----------|--------------------------------------|-----|------|
| web      | Web app for handling tasks requests. | ✅   | 80   |
| mlflow   | ML experiment tracking and registry. | ✅   | 5000 |
| minio    | S3 compatible object storage.        | ✅   | 9000 |
| postgres | Backend storage.                     | ❌   | 6543 |

> [!NOTE]
> Port values correspond to the ones published to the **host** and can be 
customized through the `.env` file.

## Getting started

1. Clone anyforecast repo
    
    ```bash
    git clone https://github.com/anyForecast/anyforecast.git
    ```

2. `cd` into the compose dir inside the project, build and run the services.
    
    ```bash
    cd anyforecast/compose
    docker-compose -f compose-core.yml up
    ```

3. **(Optional)** You can also enable the Ray backend executor 
    (`anyforecast.backend.RayBackend`) by deploying a Ray cluster using 
    ``compose-ray.yml``
    
    ```bash
    docker-compose -f compose-core.yml -f compose-ray.yml up
    ```

    Or the Celery backend executor (`anyforecast.backend.CeleryBackend`)
    
    ```bash
    docker-compose -f compose-core.yml -f compose-celery.yml up
    ```

    Or both
    ```bash
    docker-compose -f compose-core.yml -f compose-ray.yml -f compose-celery.yml up
    ```
