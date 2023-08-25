import os, logging
from brownie import network, config
from dotenv import load_dotenv
from scripts.apis.table_storage_api import TableAPI
from scripts.apis.redis_api import RedisAPI
from scripts.apis.aave_apis import AaveV2API, AaveV3API
from scripts.apis.erc20_apis import get_ERC20_metadata
from azure.identity import DefaultAzureCredential
from scripts.aave_interact import AaveInteract


class AaveERC20Tokens(AaveInteract):

    def __init__(self, network, version):
        self.network = network
        self.version = version


    def __schema_table(self, row):
        return {
            "PartitionKey": f"{network.show_active()}_aave_v{self.version}", 
            "RowKey": str(row["tokenAddress"]),
            "name": row["name"],
            "symbol": row["symbol"],
            "decimals": int(row["decimals"])
        }


    def __fulfill_azure_table(self, aave_tokens):
        for token in aave_tokens:
            token_data = get_ERC20_metadata(token)
            data = self.__schema_table(token_data)
            self.azure_table_client.insert_entity(self.azure_table_name, data)

    
    def get_metadata_erc20_tokens(self, aave_tokens):
        table_data_redis = self.redis_client.get_key(self.redis_cached_table)
        redis_missing_tokens = self._get_diff_tokens(table_data_redis, aave_tokens)
        if len(redis_missing_tokens) > 0:
            data_azure_table = self._get_azure_table_data()
            azure_missing_tokens = self._get_diff_tokens(data_azure_table, redis_missing_tokens)
            if azure_missing_tokens == []:
                self.get_from_azure_and_cache()
                return f"Tabela Azure completa e cache atualizado"
            else:
                self.__fulfill_azure_table(azure_missing_tokens)
                self.get_from_azure_and_cache()
                return f"Tabela azure preenchida e cache atualizado"
        return f"Tabela Azure cacheada"



def main(version):
    load_dotenv()
    NETWORK = network.show_active()
    ENV_VARS = config["networks"][NETWORK]
    storage_account_name = os.environ.get("STORAGE_ACCOUNT_NAME", "storage_account_name") 
    
    aave_erc20_obj = AaveERC20Tokens(network=NETWORK, version=version)
    azure_table_client = TableAPI(storage_account_name, DefaultAzureCredential())
    redis_client = RedisAPI(host='redis', port=6379)
    azure_erc20_table = "aaveERC20Tokens"
    redis_erc20_key = f"aave_tokens_{NETWORK}_V{version}"
    aave_erc20_obj.configure_services(azure_table_client, azure_erc20_table, redis_client, redis_erc20_key)

    if version == '2':
        aave_contract_addresses_provider = ENV_VARS['aave_v2_addresses_provider']
        aave_v2_obj = AaveV2API(addresses_provider=aave_contract_addresses_provider, network=NETWORK)
        aave_contract = aave_v2_obj.get_aave_pool_contract()
    elif version == '3':
        aave_contract_addresses_provider = ENV_VARS['aave_v3_addresses_provider']
        aave_v3_obj = AaveV3API(addresses_provider=aave_contract_addresses_provider, network=NETWORK)
        aave_contract = aave_v3_obj.get_aave_pool_contract()
    else:
        raise Exception("Versão inválida")

    aave_erc20_tokens = aave_contract.getReservesList()
    res = aave_erc20_obj.get_metadata_erc20_tokens(aave_erc20_tokens)
    print(res)