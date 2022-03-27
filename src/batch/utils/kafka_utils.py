import argparse, time, json
from kafka import KafkaProducer

###############################################################################################
################################    KAFKA UTILS    ############################################

def get_freq_param():
    parser = argparse.ArgumentParser()
    parser.add_argument('--freq', type=int)
    args = parser.parse_args()
    return args.freq if args.freq else 1

def json_serializer(data):
    return json.dumps(data).encode('utf-8')
    
def get_kafka_producer():
    return KafkaProducer(bootstrap_servers=['kafka:9092'], value_serializer=json_serializer)

def stream_generic_data(topic, data_gen_method, freq=1):
    producer = get_kafka_producer()
    while 1:
        producer.send(topic=topic, value=data_gen_method())
        time.sleep(1 / freq)root@4bd47ce035bb:/app/batch/utils# 


