
FROM python:3.8-slim

WORKDIR /app

COPY ./requirements.txt .

RUN apt-get update && apt-get install vim -y

RUN pip install --upgrade "pip==22.0.4" && \
    pip install -r requirements.txt


COPY ./src .

ENTRYPOINT ["sleep", "infinity"]

