FROM python:3.9
RUN pip install --upgrade pip

# Run application as non-root in the Docker container.
# Every COPY instruction needs the --chown=<user>:<group> flag to signal to
# change the file or directory owner to the tasks user (it’s root by default).
RUN adduser --disabled-login worker
USER worker
WORKDIR /home/worker

COPY --chown=worker:worker . .
RUN pip install --user -r requirements.txt
ENV PATH="/home/worker/.local/bin:${PATH}"



#CMD python -m uvicorn api.main:app --host 0.0.0.0 --port 80 --reload

