import logging
import os
import redis

from brownie import network
from itertools import combinations

from azure.identity import DefaultAzureCredential
from azure.data.tables import TableServiceClient
from azure.keyvault.secrets import SecretClient

from scripts.dm_utilities.models_uniswap import get_uniswap_factory
import pandas as pd


logging.basicConfig(level='INFO')
NULL_ADDRESS = '0x0000000000000000000000000000000000000000'

class UniswapV2PairPools:

  def __init__(self, uniswap_version, redis_client, azure_table_client, azure_table_name):
    self.version = uniswap_version
    self.redis_client = redis_client
    self.azure_table_client = azure_table_client
    self.azure_table_name = azure_table_name
    self.uniswap_factory = get_uniswap_factory(uniswap_version)


  def get_erc20_tokens(self):
    versions = ['2', '3', 'OFFICIAL']
    list_erc20_tokens = []
    for version in versions:
      table_name = f"aave_tokens_{network.show_active()}_V{version}"
      data = self.redis_client.get_key(table_name)
      list_erc20_tokens += data
    return list_erc20_tokens
  

  def get_pair_pure_combinations(self, tokens):
    list_pool_pairs =list(combinations(tokens, 2))
    return [tuple(sorted(i)) for i in list_pool_pairs]


  def get_metadata_pools(self, token_pairs):
    list_pool_addresses = [(self.uniswap_factory.getPair(tokenA, tokenB), tokenA, tokenB) for tokenA, tokenB in token_pairs]
    uniswap_pair_pool_cols = ['pool_address', 'address_token_a', 'address_token_b']
    df = pd.DataFrame(list_pool_addresses ,columns=uniswap_pair_pool_cols)
    return df

  def get_pair_name(self, df_pools, df_metadata_tokens):
    df_pair_pool = pd.merge(left=df_pools, right=df_metadata_tokens, left_on="address_token_a", right_on="RowKey", how='left')
    df_pair_pool["token_a"] = df_pair_pool['symbol'] + "_" + df_pair_pool["version"]
    df_pair_pool = df_pair_pool[["pool_address", "address_token_a", "address_token_b", "token_a"]]
    df_pair_pool = pd.merge(left=df_pair_pool, right=df_metadata_tokens, left_on="address_token_b", right_on="RowKey", how='left')
    df_pair_pool["token_b"] = df_pair_pool['symbol'] + "_" + df_pair_pool["version"]
    df_pair_pool["pair_info"] = df_pair_pool['token_a'] + "_" + df_pair_pool['token_b']
    df_pair_pool = df_pair_pool[["pair_info", "pool_address", "address_token_a", "address_token_b"]]
    return df_pair_pool


def main(version):
    
  NETWORK = network.show_active()
  KEY_VAULT = os.getenv("KEY_VAULT_NODE_NAME", "key_vault_name")
  STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT_NAME", "storage_account_name")
  TABLE_PERIPHERAL_CONTRACTS = "PeripheralSmartContracts"
  AZURE_CREDENTIAL = DefaultAzureCredential()
  REDIS_SERVER = {"host": "redis", "port": 6379}


  AKV_ENDPOINT = f"https://{KEY_VAULT}.vault.azure.net/"
  AZ_TABLES_ENDPOINT = f'https://{STORAGE_ACCOUNT}.table.core.windows.net/'
  TABLE_ADDRESSES_PROV = "CoreSmartContracts"

  aave_utility_table_name = "aaveUtilityTokens"
  
  aave_erc20_key = f"aave_tokens_{NETWORK}_V{version}"
  aave_utility_key = f"aave_utility_{NETWORK}_V{version}"

  akv_client = SecretClient(vault_url=AKV_ENDPOINT, credential=AZURE_CREDENTIAL)
  redis_client = redis.Redis(**REDIS_SERVER)

  uniswap_obj = UniswapV2PairPools(version, redis_client, azure_table_client, aave_utility_table_name)
  erc_20_tokens_data = uniswap_obj.get_erc20_tokens()
  df_erc20_tokens = pd.DataFrame(erc_20_tokens_data)
  erc20_tokens = list(map(lambda x: x["RowKey"], erc_20_tokens_data))
  list_pool_pairs = uniswap_obj.get_pair_pure_combinations(erc20_tokens)
  df_pools = uniswap_obj.get_metadata_pools(list_pool_pairs)
  print(df_pools)
  print(df_erc20_tokens)
  df_res = uniswap_obj.get_pair_name(df_pools, df_erc20_tokens)
  print(df_res)
  # uniswap_factory = get_uniswap_factory(version)
  # list_tokens = df_erc20_tokens['tokenAddress'].values
  # list_pool_pairs = get_pair_pure_combinations(list_tokens)
  # if len(list_pool_pairs) == 0: 
  #     logging.info(f"Information about tokens UNISWAP V{version} already updated")
  #     return
  # df_pools = get_metadata_pools(uniswap_factory, list_pool_pairs)
  # df_pair_pool = get_pair_name(df_pools, df_erc20_tokens)
