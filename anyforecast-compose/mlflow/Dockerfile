FROM python:3.10
RUN apt-get update && apt-get -y install libpq-dev
RUN pip install mlflow && \
    pip install boto3 && \
    pip install psycopg2


EXPOSE 5000
CMD mlflow server --backend-store-uri ${BACKEND_URI} --default-artifact-root ${ARTIFACT_ROOT} --host 0.0.0.0 --port 5000