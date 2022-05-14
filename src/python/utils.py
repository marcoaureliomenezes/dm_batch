import json
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')
    
def get_kafka_producer(host, port):
    return KafkaProducer(bootstrap_servers=[f'{host}:{port}'], value_serializer=json_serializer)

def batch_data(topic, kafka_args, data):
    producer = get_kafka_producer(**kafka_args)
    producer.send(topic=topic, value=data)