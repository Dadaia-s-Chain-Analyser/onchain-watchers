import os
from subprocess import Popen
from brownie import network
from sqlalchemy import create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.types import VARCHAR, DECIMAL
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic, KafkaAdminClient
import json

def get_partition(key, all, available):
    return 0


def get_kafka_producer(partitioner=get_partition):
    host = os.environ['KAFKA_HOST'].split(",")
    json_serializer = lambda data: json.dumps(data).encode('utf-8')
    return KafkaProducer(bootstrap_servers=host, value_serializer=json_serializer, partitioner=partitioner)


def get_kafka_consumer(topic, group_id, auto_offset_reset='latest'):
    host = os.environ['KAFKA_HOST'].split(",")
    return KafkaConsumer(topic, bootstrap_servers=host,
                        auto_offset_reset=auto_offset_reset, group_id=group_id)


def configure_kafka_topics(host, topics_config):
    admin = KafkaAdminClient(bootstrap_servers=[host])
    topics = [i for i in admin.list_topics() if i[0] != "_"]
    _ = admin.delete_topics(topics=topics) if len(topics) > 0 else None
    topic_blocks = NewTopic(name="block_clock", num_partitions=1, replication_factor=1)
    topic_txs = NewTopic(name="transactions", num_partitions=3, replication_factor=1)
    admin.create_topics(new_topics=[topic_blocks, topic_txs], validate_only=False)
    return


def get_mysql_url(database):
    service, user, pwd = [os.environ[env_var] for env_var in ('MYSQL_HOST', 'MYSQL_USER', 'MYSQL_PASSWD')]
    return f'mysql+pymysql://{user}:{pwd}@{service}:3306/{database}'


def get_mysql_engine(engine_url):
    db_engine = create_engine(engine_url)
    if not database_exists(db_engine.url):
        create_database(db_engine.url)
    return db_engine


def setup_database():
    database = network.show_active().replace("-", "_")
    url_engine = get_mysql_url(database)
    return get_mysql_engine(url_engine)


def table_exists(db_engine, table_name):
    inspector = Inspector.from_engine(db_engine)
    return table_name in inspector.get_table_names()


def divide_array(array, factor):
    return [list(filter(lambda x: x % factor == i, array)) for i in range(factor)]


def run_concurrently(commands_list):
    procs = [ Popen(i) for i in commands_list ]
    for p in procs:
        p.wait()
    return


def find_missing_keys(new_tokens, db_engine, table_name, pivot):
    df_new_tokens = pd.DataFrame(new_tokens, columns=['missing_leys'])
    df_recorded_tokens = pd.read_sql_query(f"SELECT {pivot} FROM {table_name}", con=db_engine)
    df_tokens = pd.merge(df_new_tokens, df_recorded_tokens, left_on='missing_leys', right_on=pivot, how='left')
    token_missing = df_tokens.loc[df_tokens[pivot].isnull(), 'missing_leys'].values
    return token_missing

    

def get_methods(object):
    return [method_name for method_name in dir(object)
                  if callable(getattr(object, method_name))]


