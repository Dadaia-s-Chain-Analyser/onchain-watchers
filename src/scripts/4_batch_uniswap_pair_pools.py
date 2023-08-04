import sys, os, logging
from brownie import interface
from scripts.utils.interfaces import get_uniswap_factory, get_uniswap_pair_pool
from scripts.utils.utils import setup_database, table_exists
from itertools import combinations
import pandas as pd
import numpy as np

logging.basicConfig(level='INFO')
NULL_ADDRESS = '0x0000000000000000000000000000000000000000'

def get_pair_pure_combinations(tokens):
    list_pool_pairs =list(combinations(tokens, 2))
    return [tuple(sorted(i)) for i in list_pool_pairs]


def idempotent_append(pairs, table_name, db_engine):
    if table_exists(db_engine, table_name):
        columns = ['address_token_a', 'address_token_b']
        df_pairs = pd.DataFrame(pairs ,columns=columns)
        query = f"SELECT {columns[0]}, {columns[1]} FROM {table_name} WHERE pool_address <> {NULL_ADDRESS};"
        df_pairs_recorded = pd.read_sql(query, con=db_engine)
        df_pairs_recorded['recorded'] = 'S'
        df_merged = pd.merge(df_pairs, df_pairs_recorded, on=columns, how='left')
        df_merged = df_merged[df_merged['recorded'].isnull()]
        df_missing_pair_pools = df_merged[columns].values
        list_missing_pair_pools = [(i, j) for i, j in df_missing_pair_pools]
        return list_missing_pair_pools
    return pairs


def get_metadata_pools(uniswap_factory, token_pairs):
    list_pool_addresses = [(uniswap_factory.getPair(tokenA, tokenB), tokenA, tokenB) for tokenA, tokenB in token_pairs]
    uniswap_pair_pool_cols = ['pool_address', 'address_token_a', 'address_token_b']
    df = pd.DataFrame(list_pool_addresses ,columns=uniswap_pair_pool_cols)
    return df

def get_pair_name(df_pools, df_metadata_tokens):
    df_pair_pool = pd.merge(left=df_pools, right=df_metadata_tokens, left_on="address_token_a", right_on="tokenAddress", how='left')
    df_pair_pool["token_a"] = df_pair_pool['symbol'] + "_" + df_pair_pool['description']
    df_pair_pool = df_pair_pool[["pool_address", "address_token_a", "address_token_b", "token_a"]]
    df_pair_pool = pd.merge(left=df_pair_pool, right=df_metadata_tokens, left_on="address_token_b", right_on="tokenAddress", how='left')
    df_pair_pool["token_b"] = df_pair_pool['symbol'] + "_" + df_pair_pool['description']
    df_pair_pool["pair_info"] = df_pair_pool['token_a'] + " / " + df_pair_pool['token_b']
    df_pair_pool = df_pair_pool[["pool_address", "address_token_a", "address_token_b", "pair_info"]]
    return df_pair_pool


def main(version):
    db_engine = setup_database()
    table_name = "pools_uniswap_v2"
    uniswap_factory = get_uniswap_factory(version)
    df_erc20_tokens = pd.read_sql(f"SELECT * FROM erc_20_tokens", con=db_engine)
    list_tokens = df_erc20_tokens['tokenAddress'].values
    list_pool_pairs = get_pair_pure_combinations(list_tokens)
    list_pool_pairs = idempotent_append(list_pool_pairs, table_name, db_engine)
    if len(list_pool_pairs) == 0: 
        logging.info(f"Information about tokens UNISWAP V{version} already updated")
        return
    df_pools = get_metadata_pools(uniswap_factory, list_pool_pairs)
    df_pair_pool = get_pair_name(df_pools, df_erc20_tokens)
    df_pair_pool.to_sql(table_name, con=db_engine, if_exists='append', index=False)
