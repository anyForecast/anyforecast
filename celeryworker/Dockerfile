# For more reference about this Dockerfile:
# https://medium.com/@DahlitzF/run-python-applications-as-non-root-user-in-docker-containers-by-example-cba46a0ff384
# https://fastapi.tiangolo.com/deployment/docker/

FROM python:3.9.0
RUN pip install --upgrade pip

# Run application as non-root in the Docker container.
# Every COPY instruction needs the --chown=<user>:<group> flag to signal to
# change the file or directory owner to the worker user (it’s root by default).
RUN adduser --disabled-login worker
USER worker
WORKDIR /home/worker

# Copy all content from the current directory.
COPY --chown=worker:worker . celeryworker/

# Install deps.
RUN pip install --user -r celeryworker/requirements.txt
ENV PATH="/home/worker/.local/bin:${PATH}"
