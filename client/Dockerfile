# For more reference about this dockerfile:
# https://medium.com/@DahlitzF/run-python-applications-as-non-root-user-in-docker-containers-by-example-cba46a0ff384

FROM python:3.9-slim
RUN pip install --upgrade pip

# Create new user.
RUN useradd --create-home --shell /bin/bash worker
USER worker
WORKDIR /home/worker

# Copy `client` python library and its deps.
COPY --chown=worker:worker . /home/worker/client

# Install deps and add user bin to PATH
RUN pip install --user --default-timeout=100 -r client/requirements.txt
ENV PATH="/home/worker/.local/bin:${PATH}"

