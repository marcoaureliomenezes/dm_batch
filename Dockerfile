
FROM python:3.8-slim

WORKDIR /app

COPY ./requirements.txt .

RUN apt-get update && apt-get install vim -y

RUN pip install --upgrade "pip==22.0.4" && \
    pip install -r requirements.txt



COPY ./src .
COPY ./dist ./dist

RUN chmod a+x ./run_clients.sh
RUN chmod a+x ./run_stock_markets.sh

RUN pip install ./dist/rand_engine-0.0.2.tar.gz

CMD ["sleep", "infinity"]

