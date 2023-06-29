Containerized machine learning end-to-end lifecycle system for time series 
forecasting. A manifest file (i.e., docker-compose-yml) is provided for building
all the required services which include,
 

| Name     | Description                             | GUI | Default Port |
|----------|-----------------------------------------|-----|--------------|
| mlflow   | Experiment tracking and model registry. | ✅   | 5000         |
| minio    | S3 compatible object store.             | ✅   | 9000         |
| api      | Web app for handling tasks requests.    | ✅   | 80           |
| rabbitmq | Celery message broker.                  | ❌   | 5672         |
| redis    | Celery backend.                         | ❌   | 6379         |
| flower   | Monitors Celery jobs and workers.       | ✅   | 5672         |
| postgres | Backend storage.                        | ❌   | 6543         |

> **Note**
> 
> Default Port values correspond to the ones published to the **host** and can be customized through the .env file.

> **Note**
> This is a note






Build in GCP
------------
(Comming soon!)


Run locally
-----------
```
docker-compose up -d --build
```
Open your browser to http://localhost:<*port*> to view any of the available UI's.