# For more reference about this dockerfile:
# https://medium.com/@DahlitzF/run-python-applications-as-non-root-user-in-docker-containers-by-example-cba46a0ff384
# https://fastapi.tiangolo.com/deployment/docker/

FROM python:3.9
RUN pip install --upgrade pip


# Copy `api` python library and its deps.
COPY . /home/worker/forecast_api

# Install deps.
RUN pip install -r /home/worker/forecast_api/requirements.txt

# Install skorch_forecasting and pyrm.
RUN pip install /home/worker/forecast_api/repos/skorch_forecasting/.
RUN pip install /home/worker/forecast_api/repos/pyrm/.
