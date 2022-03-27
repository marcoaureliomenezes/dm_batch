
FROM python:3.8-alpine
RUN mkdir /app
ADD . /app
WORKDIR /app

RUN pip install python-dotenv faker pymongo yahooquery

RUN pip install ppython-dotenv==0.20.0 && \
    pip install tweepy==4.8.0 && \
    pip install pandas==1.4.1 && \
    pip install pymysql==1.0.2 && \
    pip install sqlalchemy==1.4.32 && \
    pip install kafka-python==2.0.2 && \
    pip install pymongo==4.0.2 && \
    pip install yahooquery 2.2.15

CMD ["sleep", "infinity"]

