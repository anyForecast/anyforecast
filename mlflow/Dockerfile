FROM python:3.9
RUN apt-get update && apt-get -y install libpq-dev
RUN pip install mlflow==1.30.0 && \
    pip install boto3==1.24.59 && \
    pip install psycopg2