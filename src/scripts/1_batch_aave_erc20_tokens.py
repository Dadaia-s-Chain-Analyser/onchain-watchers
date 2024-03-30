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

def format_erc20_data(row, network, version): 
  return {
    "PartitionKey": f"{network}_aave_v{version}",
    "RowKey": str(row["tokenAddress"]),
    "name": row["name"],
    "symbol": row["symbol"],
    "decimals": int(row["decimals"])
  }


def main(version):

  NETWORK = network.show_active()
  TABLE_ADDR_PROV = "CoreSmartContracts"
  TABLE_AAVE_TOKENS = "AaveERC20Tokens"

  AKV_URL = f'https://{os.getenv("KEY_VAULT_NODE_NAME")}.vault.azure.net/'
  AZ_TBLS_URL = f'https://{os.getenv("STORAGE_ACCOUNT_NAME")}.table.core.windows.net/'
  REDIS_SERVER = dict(host="redis", port=6379)
  AZURE_CREDENTIAL = DefaultAzureCredential()

  # Instancia serviços Key Vault para API KEY, Azure Tables e Redis para dados de tokens listados na Aave
  akv_client = SecretClient(vault_url=AKV_URL, credential=AZURE_CREDENTIAL)
  az_tables_client = TableServiceClient(endpoint=AZ_TBLS_URL, credential=AZURE_CREDENTIAL)
  redis_client = RedisClient(**REDIS_SERVER)
  erc20_actor = ERC20API()

  redis_key = f"aave_tokens_{NETWORK}_V{version}" #  Chave do Redis. Ex: aave_tokens_goerli_V2, aave_tokens_mainnet_V3, ...

  az_table_providers = az_tables_client.get_table_client(TABLE_ADDR_PROV)
  az_table_aave_tokens = az_tables_client.get_table_client(TABLE_AAVE_TOKENS)

  az_table_providers_data = get_azure_table(az_table_providers, f"PartitionKey eq '{NETWORK}' and RowKey eq 'aave'")[0]
  aave_data_provider =  az_table_providers_data[f"aave_v{version}_data_provider"]

  AaveDataProvider = getattr(sys.modules[__name__], f"AaveV{version}DataProvider")
  aave_data_provider_actor = AaveDataProvider(data_provider_addr=aave_data_provider, network=NETWORK)

  # DEFINIÇÃO DE ALGUMAS LAMBDAS
  az_table_aave_tokens = az_tables_client.get_table_client(TABLE_AAVE_TOKENS)
  query_aave_listed_tokens = f"PartitionKey eq '{NETWORK}_aave_v{version}'"

  cached_listed_tokens = redis_client.get_key_obj(redis_key)
  cached_listed_tokens = map(lambda x: x["RowKey"], cached_listed_tokens)

  listed_tokens = aave_data_provider_actor.get_all_reserve_tokens()
  listed_tokens_addresses = list(map(lambda x: x[1], listed_tokens))

  cached_missing_listed_tokens = list(set(listed_tokens_addresses) - set(cached_listed_tokens))
  if len(cached_missing_listed_tokens) > 0:
    data_azure_table = get_azure_table(az_table_aave_tokens, query_aave_listed_tokens)
    stored_aave_tokens = map(lambda x: x["RowKey"], data_azure_table)
    stored_missing_tokens = list(set(listed_tokens_addresses) - set(stored_aave_tokens))
    if len(stored_missing_tokens) > 0:
      for token in stored_missing_tokens:
        token_raw_data = erc20_actor.get_ERC20_metadata(token)  # 1 REQUEST TO BLOCKCHAIN NODE
        token_data = format_erc20_data(token_raw_data, NETWORK, version)
        az_table_aave_tokens.upsert_entity(token_data)
      data_azure_table = get_azure_table(az_table_aave_tokens, query_aave_listed_tokens)
      print(f"Tabela {TABLE_AAVE_TOKENS} atualizada com sucesso")
    else: print(f"Tabela {TABLE_AAVE_TOKENS} está completa")
    redis_client.insert_key_obj(redis_key, data_azure_table)
    print(f"Cache atualizado com sucesso")
  else:
    print(f"Cache está OK")
