FROM python:3.10

RUN pip install --upgrade pip

ENV PYTHONUNBUFFERED True
ENV PORT 8080
EXPOSE ${PORT}

RUN adduser --disabled-login worker
USER worker
WORKDIR /home/worker/

ENV APP_HOME /home/worker/anyforecast
COPY --chown=worker:worker . ${APP_HOME}

ENV PATH /home/worker/.local/bin:${PATH}
RUN pip install anyforecast/

CMD exec anyforecast web start --host 0.0.0.0 --port ${PORT}
