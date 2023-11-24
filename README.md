Automated ⚙️ and distributed time series forecasting 🚀.

anyforecast is a containerized machine learning end-to-end system for time 
series forecasting. 


# Run locally
A docker-compose.yml file is provided for building the required services,
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

CD into the project, build and run the services.
```
cd anyforecast
docker-compose -f compose/docker-compose.yml up
```
