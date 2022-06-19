# For more reference about this dockerfile:
# https://medium.com/@DahlitzF/run-python-applications-as-non-root-user-in-docker-containers-by-example-cba46a0ff384
# https://fastapi.tiangolo.com/deployment/docker/

FROM python:3.9
RUN pip install --upgrade pip

# Create new user.
RUN useradd --create-home --shell /bin/bash worker
USER worker
WORKDIR /home/worker

# Copy `api` python library and its deps.
COPY --chown=worker:worker . /home/worker/api

# Install deps and add user bin to PATH
RUN pip install --user -r api/requirements.txt
RUN pip install --user api/mooncake/.
ENV PATH="/home/worker/.local/bin:${PATH}"
