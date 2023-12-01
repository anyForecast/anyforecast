Automated ⚙️ and distributed time series forecasting 🚀.

anyforecast is a containerized machine learning end-to-end system for time 
series forecasting. 


# Run locally
A ``compose-core.yml`` file is provided for building the required services,
which include

| Name     | Description                          | GUI | Port |
|----------|--------------------------------------|-----|------|
| web      | Web app for handling tasks requests. | ✅   | 80   |
| mlflow   | ML experiment tracking and registry. | ✅   | 5000 |
| minio    | S3 compatible object storage.        | ✅   | 9000 |
| postgres | Backend storage.                     | ❌   | 6543 |

> [!NOTE]
> Port values correspond to the ones published to the **host** and can be customized through the .env file.

First, clone this repo with
```
git clone https://github.com/anyForecast/anyforecast.git
```

`cd` into the compose dir inside the project, build and run the services.
```
cd anyforecast/compose
docker-compose -f compose-core.yml up
```

You can also enable the Ray backend executor 
(`anyforecast.backend.RayBackend`) by deploying a Ray cluster using 
``compose-ray.yml``
```
docker-compose -f compose-core.yml -f compose-ray.yml up
```

Or the Celery backend executor (`anyforecast.backend.CeleryBackend`)
```
docker-compose -f compose-core.yml -f compose-celery.yml up
```

Or both
```
docker-compose -f compose-core.yml -f compose-ray.yml -f compose-celery.yml up
```

