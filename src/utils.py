import json, os, logging
from functools import reduce
from sqlalchemy.engine.reflection import Inspector
from kafka import KafkaProducer
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import create_engine

def handle_parms(parms, required_parms, method, method_args):
    logging.basicConfig(level=logging.INFO)
    real_parms = set(parms.keys())
    if real_parms.intersection(set(required_parms)) == set(required_parms):
        return method(**method_args)
    logging.info(f"Parms ({reduce(lambda a, b: f'{a}, {b}', required_parms)}) are required")


def table_exists(db_engine, table_name):
    inspector = Inspector.from_engine(db_engine)
    return table_name in inspector.get_table_names()

def get_mysql_engine(database):
    url_db_engine = f"mysql+pymysql://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASS')}@{os.getenv('MYSQL_SERVICE')}/{database}"
    db_engine = create_engine(url_db_engine)
    if not database_exists(db_engine.url):
        create_database(db_engine.url)
    return create_engine(url_db_engine, encoding="utf8")


def json_serializer(data):
    return json.dumps(data).encode('utf-8')
    
def get_kafka_producer(host, port):
    return KafkaProducer(bootstrap_servers=[f'{host}:{port}'], value_serializer=json_serializer)

def batch_data(topic, kafka_args, data):
    producer = get_kafka_producer(**kafka_args)
    producer.send(topic=topic, value=data)