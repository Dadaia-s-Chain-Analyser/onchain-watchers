from brownie import network
from scripts.apis.aave_apis import get_price_oracle
from scripts.apis.redis_api import RedisAPI
import os, json
from scripts.apis.kafka_api import KafkaClient

class AavePricesStreamer:

    def __init__(self, redis_client, kafka_client, version):
        self.redis_key = f"aave_tokens_{network.show_active()}_V{version}"
        self.redis_client = redis_client
        self.version = version
        self.kafka_client = kafka_client


    def get_tokens(self):
        erc20_aave_tokens = self.redis_client.get_key(self.redis_key)
        erc20_aave_tokens = list(map(lambda x: (x['RowKey'], x.get("symbol", x["RowKey"])), erc20_aave_tokens))
        return erc20_aave_tokens


    def get_token_prices(self, oracle_contract):
        erc_tokens_list = self.get_tokens() 
        erc20_aave_tokens = list(map(lambda x: x[0], erc_tokens_list))
        assets_new_prices = oracle_contract.getAssetsPrices(erc20_aave_tokens)
        token_prices = {erc_tokens_list[i][1]: assets_new_prices[i] for i in range(len(erc20_aave_tokens))}
        return token_prices 


    def my_function(self, oracle_contract, redis_output_key):
        token_prices = self.get_token_prices(oracle_contract)
        data = self.redis_client.get_dict(redis_output_key)
        vect = []
        for k, v in token_prices.items():
            if data.get(k) != v:
                row = {"token": k, "price": v}
                vect.append(row)
                data[k] = v
        self.redis_client.register_key(redis_output_key, data)
        yield vect
     

def main(version):

    kafka_endpoint = os.environ['KAFKA_ENDPOINT']
    consumer_group = os.environ['CONSUMER_GROUP']

    topic_blocks = f"{network.show_active()}_{os.environ['TOPIC_INPUT']}"
    topic_aave_prices = f"{network.show_active()}_{os.environ['TOPIC_OUTPUT']}"

    redis_client = RedisAPI(host='redis', port=6379)
    redis_key = "cache_daqui2"
    kafka_client = KafkaClient(connection_str=kafka_endpoint)

    aave_price_streamer = AavePricesStreamer(redis_client, kafka_client, version)
    kafka_client.create_idempotent_topic(topic=topic_aave_prices)

    producer = kafka_client.create_producer()
    consumer = kafka_client.create_consumer(topic=topic_blocks, consumer_group=consumer_group)

    aave_price_streamer.get_tokens()
    oracle_contract = get_price_oracle(version)
    for msg in consumer:
        for data in aave_price_streamer.my_function(oracle_contract, redis_key):
            if len(data) > 0:
                for token_price in data:
                    producer.send(topic=topic_aave_prices, value=token_price)
     