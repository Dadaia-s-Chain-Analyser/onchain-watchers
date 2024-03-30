from functools import reduce
import logging
import os
import sys

from brownie import network
from azure.identity import DefaultAzureCredential
from azure.data.tables import TableServiceClient
from azure.keyvault.secrets import SecretClient

from scripts.dm_utilities.models_aave_v2 import AaveV2DataProvider
from scripts.dm_utilities.models_aave_v3 import AaveV3DataProvider
from scripts.dm_utilities.models_erc20 import ERC20API

from scripts.dm_utilities.redis_client import RedisClient


def get_azure_table(az_table_client, query):
  return [i for i in az_table_client.query_entities(query)]


def format_erc20_data(row, father_token, network, version):
  return {
    "PartitionKey": f"{network}_aave_v{version}",
    "RowKey": str(row["tokenAddress"]),
    "FatherToken": str(father_token),
    "version": f"v{version}",
    "name": row["name"],
    "symbol": row["symbol"],
    "decimals": int(row["decimals"])
  }

def main(version):

  NETWORK = network.show_active()
  TABLE_ADDR_PROV = "CoreSmartContracts"
  TABLE_AAVE_TOKENS = "AaveERC20Tokens"
  TABLE_AAVE_UTILITY_TOKENS = "aaveUtilityTokens"
  
  AZ_TBLS_URL = f'https://{os.getenv("STORAGE_ACCOUNT_NAME")}.table.core.windows.net/'
  REDIS_SERVER = dict(host="redis", port=6379)
  AZURE_CREDENTIAL = DefaultAzureCredential()

  az_tables_client = TableServiceClient(endpoint=AZ_TBLS_URL, credential=AZURE_CREDENTIAL)
  redis_client = RedisClient(**REDIS_SERVER)
  erc20_actor = ERC20API()

  redis_utility_tokens_key = f"aave_utility_tokens_{NETWORK}_V{version}"

  az_table_providers = az_tables_client.get_table_client(TABLE_ADDR_PROV)
  az_table_aave_utility_tokens = az_tables_client.get_table_client(TABLE_AAVE_UTILITY_TOKENS)
  az_table_providers_data = get_azure_table(az_table_providers, f"PartitionKey eq '{NETWORK}' and RowKey eq 'aave'")[0]
  aave_data_provider =  az_table_providers_data[f"aave_v{version}_data_provider"] 
  AaveDataProvider = getattr(sys.modules[__name__], f"AaveV{version}DataProvider")
  aave_data_provider_actor = AaveDataProvider(data_provider_addr=aave_data_provider, network=NETWORK)

  reserve_tokens = aave_data_provider_actor.get_all_reserve_tokens()
  reserve_tokens = list(map(lambda x: x[1], reserve_tokens))
  
  query_aave_utility_tokens = f"PartitionKey eq '{NETWORK}_aave_v{version}'"
  redis_utility_tokens_key = f"aave_utility_tokens_{NETWORK}_V{version}" # Chave Redis para cache dos tokens ERC20
  cached_utility_tokens = redis_client.get_key_obj(redis_utility_tokens_key)

  if len(cached_utility_tokens) == 0:

    data_azure_table = get_azure_table(az_table_aave_utility_tokens, query_aave_utility_tokens)
    stored_utility_tokens = map(lambda x: x["RowKey"], data_azure_table)
    listed_utility_tokens = list(map(lambda token: (token, aave_data_provider_actor.get_reserve_token_addresses(token)), reserve_tokens))
    utility_tokens = reduce(lambda a, b: a + b, [list(i[1]) for i in listed_utility_tokens]) if len(listed_utility_tokens) > 0 else []
  
    missing_utility_tokens = list(set(utility_tokens) - set(stored_utility_tokens))
    if len(missing_utility_tokens) > 0:
      print(f"MISSING STORED utility tokens: {missing_utility_tokens}")
      for token in missing_utility_tokens:
        token_raw_data = erc20_actor.get_ERC20_metadata(token)  # 1 REQUEST TO BLOCKCHAIN NODE
        father_token = list(filter(lambda x: token in x[1], listed_utility_tokens))[0][0]
        utility_token_data = format_erc20_data(token_raw_data, father_token, NETWORK, version)
        az_table_aave_utility_tokens.upsert_entity(utility_token_data)
        data_azure_table = get_azure_table(az_table_aave_utility_tokens, query_aave_utility_tokens)
      print(f"Tabela {TABLE_AAVE_UTILITY_TOKENS} atualizada com sucesso")
    else:
      print(f"Tabela {TABLE_AAVE_UTILITY_TOKENS} está completa")
    data_to_cache = list(map(lambda x: (x["RowKey"], x["FatherToken"], x["symbol"]), data_azure_table))
    redis_client.insert_key_obj(redis_utility_tokens_key, data_to_cache)
  else:
    print(f"Chave redis_utility_tokens_key está completa")