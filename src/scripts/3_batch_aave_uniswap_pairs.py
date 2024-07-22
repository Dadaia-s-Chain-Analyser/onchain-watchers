import logging
import os


from brownie import network
from itertools import combinations

from azure.identity import DefaultAzureCredential
from azure.data.tables import TableServiceClient
from azure.keyvault.secrets import SecretClient

from scripts.dm_utilities.models_uniswap import UniswapFactory
from scripts.dm_utilities.redis_client import RedisClient


import pandas as pd



NULL_ADDRESS = '0x0000000000000000000000000000000000000000'

# class UniswapV2PairPools:

#   def __init__(self, uniswap_version, redis_client, azure_table_client, azure_table_name):
#     self.version = uniswap_version
#     self.redis_client = redis_client
#     self.azure_table_client = azure_table_client
#     self.azure_table_name = azure_table_name
#     self.uniswap_factory = get_uniswap_factory(uniswap_version)
  

#   def get_pair_pure_combinations(self, tokens):
#     list_pool_pairs =list(combinations(tokens, 2))
#     return [tuple(sorted(i)) for i in list_pool_pairs]


#   def get_metadata_pools(self, token_pairs):
#     list_pool_addresses = [(self.uniswap_factory.getPair(tokenA, tokenB), tokenA, tokenB) for tokenA, tokenB in token_pairs]
#     uniswap_pair_pool_cols = ['pool_address', 'address_token_a', 'address_token_b']
#     df = pd.DataFrame(list_pool_addresses ,columns=uniswap_pair_pool_cols)
#     return df




def get_azure_table(az_table_client, query):
  return [i for i in az_table_client.query_entities(query)]


def format_erc20_data(row, network, version): 
  return {
    "PartitionKey": f"{network}_uniswap_v{version}",
    "RowKey": row["pair_name"],
    "token_a": str(row["token_a"]),
    "token_b": str(row["token_b"]),
    "symbol_a": row["symbol_a"],
    "symbol_b": row["symbol_b"],
    "pair_pool_address": str(row["pair_pool_address"])
  }

def get_pair_pure_combinations(tokens):
  tokens = list(map(lambda x: x["RowKey"], tokens))
  print(tokens)
  list_pool_pairs =list(combinations(tokens, 2))
  list_pair_pure_combinations = list(map(lambda x: {"token_b": x[0], "token_a": x[1]}, list_pool_pairs))
  return pd.DataFrame(list_pair_pure_combinations)
  

def get_df_pair_pure_combinations(tokens):
    print(tokens)
    #tokens = list(map(lambda x: x["RowKey"], tokens))
    df_tokens = pd.DataFrame(tokens)
    df_pairs = get_pair_pure_combinations(tokens)
    df_pairs["symbol_a"] = df_pairs.merge(df_tokens, left_on="token_a", right_on="RowKey", how="left")["symbol"]
    df_pairs["symbol_b"] = df_pairs.merge(df_tokens, left_on="token_b", right_on="RowKey", how="left")["symbol"]
    df_pairs["pair_name"] = df_pairs["symbol_a"] + "-" + df_pairs["symbol_b"]
    return df_pairs

8553.209665995958522073
12141640

def main(version):
    
  NETWORK = network.show_active()
  TABLE_UNISWAP_POOLS = "AaveUniswapPairPools"
  TABLE_ADDR_PROV = "CoreSmartContracts"
  AKV_URL = f'https://{os.getenv("KEY_VAULT_NODE_NAME")}.vault.azure.net/'
  AZ_TBLS_URL = f'https://{os.getenv("STORAGE_ACCOUNT_NAME")}.table.core.windows.net/'
  REDIS_SERVER = dict(host="redis", port=6379)
  AZURE_CREDENTIAL = DefaultAzureCredential()

  az_tables_client = TableServiceClient(endpoint=AZ_TBLS_URL, credential=AZURE_CREDENTIAL)
  az_table_providers = az_tables_client.get_table_client(TABLE_ADDR_PROV)
  az_table_uniswap_pools = az_tables_client.get_table_client(TABLE_UNISWAP_POOLS)
  
  az_table_providers_data = get_azure_table(az_table_providers, f"PartitionKey eq '{NETWORK}' and RowKey eq 'uniswap'")[0]
  uniswap_factory_addr =  az_table_providers_data[f"uniswap_v{version}_factory"]
  print(uniswap_factory_addr)

  aave_erc20_key = f"aave_tokens_{NETWORK}_V{version}"
  aave_erc20_pairs_key = f"aave_tokens_uniswap_pairs{NETWORK}_V{version}"
  redis_client = RedisClient(**REDIS_SERVER)

  cached_erc20_tokens = redis_client.get_key_obj(aave_erc20_key)
  
  df_pairs = get_df_pair_pure_combinations(cached_erc20_tokens)

  print(df_pairs)
  uniswap_factory = UniswapFactory(uniswap_factory_addr, NETWORK, version)

  cached_uniswap_pair_pools = redis_client.get_key_obj(aave_erc20_pairs_key)
  cached_uniswap_pair_pools = list(map(lambda x: x["RowKey"], cached_uniswap_pair_pools))

  if len(cached_uniswap_pair_pools) != len(cached_erc20_tokens):
    data_azure_table = get_azure_table(az_table_uniswap_pools, f"PartitionKey eq '{NETWORK}_uniswap_v{version}'")
    stored_uniswap_pair_pools = list(map(lambda x: x["RowKey"], data_azure_table))

    
    dict_pairs = df_pairs.to_dict(orient='records')
    uniswap_pair_pools = map(lambda x: x["pair_name"], dict_pairs)
    stored_missing_uniswap_pair_pools = list(set(uniswap_pair_pools) - set(stored_uniswap_pair_pools))
    print(stored_missing_uniswap_pair_pools)
    if len(stored_missing_uniswap_pair_pools) > 0:
      for pair in stored_missing_uniswap_pair_pools:
        row = df_pairs[df_pairs["pair_name"] == pair].iloc[0]
        row = row.to_dict()
        row["pair_pool_address"] = uniswap_factory.get_uniswap_pair(row["token_a"], row["token_b"])
        data_azure_table = format_erc20_data(row, NETWORK, version)
        az_table_uniswap_pools.upsert_entity(data_azure_table)
      print(f"Tabela {TABLE_UNISWAP_POOLS} atualizada com sucesso")
      data_azure_table = get_azure_table(az_table_uniswap_pools, f"PartitionKey eq '{NETWORK}_uniswap_v{version}'")
    else: print(f"Tabela {TABLE_UNISWAP_POOLS} está completa")
    data_to_cache = list(map(lambda x: {"RowKey": x["pair_name"], "token_a": x["token_a"], "token_b": x["token_b"]}, dict_pairs))
    redis_client.insert_key_obj(aave_erc20_pairs_key, data_to_cache)
    print(f"Cache atualizado com sucesso")
  else:
    print(f"Cache já atualizado")

    # for token in stored_missing_tokens:
    #   symbol, address = list(filter(lambda x: x[1] == token, listed_tokens))[0]
    #   data_azure_table = format_erc20_data(erc20_data, symbol, address, NETWORK, version)
    #   az_table_aave_tokens.upsert_entity(data_azure_table)
    #   redis_client.set_key_obj(redis_key, data_azure_table)
    #   logging.info(f"Token {symbol} address {address} inserted in Azure Table {TABLE_AAVE_TOKENS}")
  # df_pairs["symbol_a"] = df_pairs.merge(df_tokens, left_on="token_a", right_on="RowKey", how="left")["symbol"]
  # df_pairs["symbol_b"] = df_pairs.merge(df_tokens, left_on="token_b", right_on="RowKey", how="left")["symbol"]
  # df_pairs["pair_name"] = df_pairs["symbol_a"] + "-" + df_pairs["symbol_b"]
  # df_pairs["pair_pool_address"] = df_pairs.apply(lambda x: uniswap_factory.get_uniswap_pair(x["token_a"], x["token_b"]), axis=1)

  # print(df_pairs[df_pairs["pair_pool_address"] != NULL_ADDRESS])


  # erc_20_tokens_data = uniswap_obj.get_erc20_tokens()
  # df_erc20_tokens = pd.DataFrame(erc_20_tokens_data)
  # erc20_tokens = list(map(lambda x: x["RowKey"], erc_20_tokens_data))
  # list_pool_pairs = uniswap_obj.get_pair_pure_combinations(erc20_tokens)
  # df_pools = uniswap_obj.get_metadata_pools(list_pool_pairs)
  # print(df_pools)
  # print(df_erc20_tokens)
  # df_res = uniswap_obj.get_pair_name(df_pools, df_erc20_tokens)
  # print(df_res)
  # uniswap_factory = get_uniswap_factory(version)
  # list_tokens = df_erc20_tokens['tokenAddress'].values
  # list_pool_pairs = get_pair_pure_combinations(list_tokens)
  # if len(list_pool_pairs) == 0: 
  #     logging.info(f"Information about tokens UNISWAP V{version} already updated")
  #     return
  # df_pools = get_metadata_pools(uniswap_factory, list_pool_pairs)
  # df_pair_pool = get_pair_name(df_pools, df_erc20_tokens)
