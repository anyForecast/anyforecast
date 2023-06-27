FROM python:3.9
RUN pip install --upgrade pip
RUN adduser --disabled-login worker
USER worker
WORKDIR /home/worker
COPY --chown=worker:worker . /home/worker
RUN pip install --user --no-warn-script-location -r requirements.txt

# Set environment.
ENV PATH="/home/worker/.local/bin:${PATH}"
ENV AWS_ACCESS_KEY_ID=celery
ENV AWS_SECRET_ACCESS_KEY=celery
ENV WR_S3_ENDPOINT_URL=http://minio:9000
ENV MLFLOW_S3_ENDPOINT_URL=http://minio:9000
ENV MLFLOW_TRACKING_URI=http://mlflow:5000
ENV BROKER_URL=amqp://rabbitmq:5672
ENV CELERY_RESULT_BACKEND=redis://redis:6379/0


CMD celery -A celeryapp worker --loglevel=INFO

