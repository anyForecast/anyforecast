FROM python:3.9
RUN apt-get update && apt-get -y install libpq-dev
RUN pip install mlflow && \
    pip install boto3 && \
    pip install psycopg2