import os
import redis
from brownie import network, config
from azure.identity import DefaultAzureCredential
from azure.data.tables import TableServiceClient
from azure.keyvault.secrets import SecretClient

from scripts.dm_utilities.models_aave import AaveV2API, AaveV3API
from scripts.dm_utilities.models_erc20 import ERC20API


class AaveERC20Tokens:

  def __init__(self, redis_client, azure_table_client, azure_table_name, version):
    self.redis_client = redis_client
    self.azure_table_client = azure_table_client
    self.azure_table_name = azure_table_name
    self.version = version
    self.redis_cached_table = f"aave_tokens_{network.show_active()}_V{version}"


  def __schema_table(self, row):
    return {
      "PartitionKey": network.show_active(), 
      "RowKey": str(row["tokenAddress"]), 
      "version": f"{self.version}",
      "name": row["name"],
      "symbol": row["symbol"],
      "decimals": int(row["decimals"])
    }
    

  def __get_azure_table_data(self):
    query = f"PartitionKey eq '{network.show_active()}' and version eq '{self.version}'"
    return self.azure_table_client.query_table(self.azure_table_name, query=query)


  def __fulfill_azure_table(self, aave_tokens):
    for token in aave_tokens:
      token_data = get_ERC20_metadata(token)
      data = self.__schema_table(token_data)
      self.azure_table_client.insert_entity(self.azure_table_name, data)


  def __get_diff_tokens(self, stored_data, aave_tokens):
    stored_aave_tokens = map(lambda x: x["RowKey"], stored_data)
    diff_tokens = list(set(aave_tokens) - set(stored_aave_tokens))
    return diff_tokens
    

  def __get_aave_reserve_tokens(self):
    link_token = config["networks"][network.show_active()]["link_token"]
    weth_token = config["networks"][network.show_active()]["weth_token"]
    uni_token = config["networks"][network.show_active()]["uni_token"]
    tokens = [link_token, weth_token, uni_token]
    return tokens
    

  def get_metadata_erc20_tokens(self):
    table_data_redis = self.redis_client.get_key(self.redis_cached_table)
    aave_tokens = self.__get_aave_reserve_tokens()
    redis_missing_tokens = self.__get_diff_tokens(table_data_redis, aave_tokens)
    if len(redis_missing_tokens) > 0:
      data_azure_table = self.__get_azure_table_data()
      azure_missing_tokens = self.__get_diff_tokens(data_azure_table, redis_missing_tokens)
      if azure_missing_tokens == []:
        self.redis_client.register_key(self.redis_cached_table, data_azure_table)
        return f"Tabela Azure completa e cache atualizado"
      else:
        self.__fulfill_azure_table(azure_missing_tokens)
        table_data_azure = self.__get_azure_table_data()
        self.redis_client.register_key(self.redis_cached_table, table_data_azure)
        return f"Tabela azure preenchida e cache atualizado"
    return f"Tabela Azure cacheada"


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


  erc20_table_name = "aaveERC20Tokens"
  akv_client = SecretClient(vault_url=AKV_ENDPOINT, credential=AZURE_CREDENTIAL)
  redis_client = redis.Redis(**REDIS_SERVER)

  az_tables_client = TableServiceClient(endpoint=AZ_TABLES_ENDPOINT, credential=AZURE_CREDENTIAL)
  az_table_providers = az_tables_client.get_table_client(TABLE_ADDRESSES_PROV)

  aave_erc20_obj = AaveERC20Tokens(redis_client, az_table_providers, erc20_table_name, version)
  #azure_table_client.create_table(erc20_table_name)
  res = aave_erc20_obj.get_metadata_erc20_tokens()
  print(res)