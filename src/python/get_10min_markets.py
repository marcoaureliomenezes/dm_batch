import requests, time, os, argparse
import pandas as pd
from dotenv import load_dotenv
from utils import  get_kafka_producer



                

load_dotenv()

def get_currencie_data(API_KEY):
    url, headers = ('https://rest.coinapi.io/v1/assets', {'X-CoinAPI-Key' : API_KEY})
    response = requests.get(url, headers=headers)
    data_coins_filtered = list(filter(lambda x: x["volume_1day_usd"] > 10**3, response.json()))
    return data_coins_filtered


def produce_currencies_data(api_key, kafka_args):
    producer = get_kafka_producer(**kafka_args)
    
    data_coins = get_currencie_data(api_key)
    timestamp = time.time()
    for coin in data_coins:
        coin["timestamp"] = timestamp
        producer.send(topic='currencies_data', value=coin)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='python arguments')
    parser.add_argument('--host', help= '', required=True)
    parser.add_argument('--port', help= '', required=True)
    args = parser.parse_args()
    parm_host, parm_port= (args.host, args.port)
    kafka_args = {"host": parm_host, "port": parm_port}
    API_KEY = os.getenv("COIN_API_TOKEN")
    produce_currencies_data(API_KEY, kafka_args)