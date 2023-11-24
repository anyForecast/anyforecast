FROM python:3.10

RUN adduser --system --no-create-home nonroot

RUN pip install --upgrade pip

ENV PYTHONUNBUFFERED True
ENV PORT 8080

ENV APP_HOME /app
RUN git clone https://github.com/anyForecast/anyforecast.git $APP_HOME
WORKDIR $APP_HOME
COPY . ./

RUN pip install .

USER nonroot
EXPOSE ${PORT}
CMD exec anyforecast web start --host 0.0.0.0 --port ${PORT}
