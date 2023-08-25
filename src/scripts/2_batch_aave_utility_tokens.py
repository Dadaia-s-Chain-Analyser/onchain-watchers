import logging
from dotenv import load_dotenv
from brownie import config, network
from azure.identity import DefaultAzureCredential
from scripts.apis.table_storage_api import TableAPI
from scripts.apis.redis_api import RedisAPI
from scripts.apis.aave_apis import AaveV2API, AaveV3API
from scripts.apis.erc20_apis import get_ERC20_metadata
from scripts.aave_interact import AaveInteract



class AaveUtilityTokens(AaveInteract):

    def __init__(self, network, version, aave_api):
        self.network = network
        self.aave_api = aave_api
        self.aave_contract = aave_api.get_aave_pool_contract()
        self.version = version


    def __schema_table(self, row, father_token):
        return {
            "PartitionKey": f"{self.network}_aave_v{self.version}",
            "RowKey": str(row["tokenAddress"]),
            "FatherToken": str(father_token),
            "version": f"v{self.version}",
            "name": row["name"],
            "symbol": row["symbol"],
            "decimals": int(row["decimals"])
        }


    def get_reserve_tokens(self, token):
        list_type_tokens = ["aTokenAddress", "stableDebtTokenAddress", "variableDebtTokenAddress"]
        utility_tokens_indexes = self.aave_api.get_indexes_datatypes(list_type_tokens)
        tokens = [self.aave_contract.getReserveData(token)[utility_tokens_indexes[type_token]] for type_token in list_type_tokens]
        return {"tokenAddress": token, **{list_type_tokens[i]: tokens[i] for i in range(len(tokens))}}
    

    def __get_list_utility_tokens(self, data_erc20_tokens):
        list_erc20_tokens = list(map(lambda x: x["RowKey"], data_erc20_tokens))
        reserve_tokens = [self.get_reserve_tokens(token) for token in list_erc20_tokens]
        get_utility_tokens = lambda x: (x['tokenAddress'], x["aTokenAddress"], x["stableDebtTokenAddress"], x["variableDebtTokenAddress"])
        list_utility_tokens = list(map(get_utility_tokens, reserve_tokens))
        return list_utility_tokens


    def __get_metadata_utility_tokens(self, list_utility_tokens):
        for dict_row in list_utility_tokens:
            for j in range(1, len(dict_row)):
                token = (dict_row[0], dict_row[j])
                token_data = get_ERC20_metadata(token[1])
                token_data = self.__schema_table(token_data, token[0])
                yield token_data


    def __fulfill_azure_table(self, list_utility_tokens):
        for token_data in self.__get_metadata_utility_tokens(list_utility_tokens):
            self.azure_table_client.insert_entity(self.azure_table_name, token_data)
          


    def get_from_azure_and_cache(self):
        data_azure_table = self._get_azure_table_data()
        self.redis_client.register_key(self.redis_cached_table, data_azure_table)


    def cache_aave_utility_tokens(self, redis_utility_tokens, aave_utility_tokens_data):
        aave_utility_tokens = [(token[0], i) for token in aave_utility_tokens_data for i in token[1:]]
        utility_tokens = [j for i, j in aave_utility_tokens]
        redis_missing_tokens = self._get_diff_tokens(redis_utility_tokens, utility_tokens)
        if len(redis_missing_tokens) > 0:
            data_azure_table = self._get_azure_table_data()
            azure_missing_tokens = self._get_diff_tokens(data_azure_table, utility_tokens)
            if len(azure_missing_tokens) == 0:
                self.get_from_azure_and_cache()
                return f"Tabela Azure completa e cache atualizado"
            else:
                aave_utility_tokens = list(filter(lambda x: x[1] in azure_missing_tokens, aave_utility_tokens))
                self.__fulfill_azure_table(aave_utility_tokens)
                self.get_from_azure_and_cache()
                return f"Tabela azure preenchida e cache atualizado"
        return f"Tabela Azure cacheada"    


    def run(self, redis_erc20_tokens):
        redis_utility_tokens = self.redis_client.get_key(self.redis_cached_table)
        aave_utility_tokens_data = self.__get_list_utility_tokens(redis_erc20_tokens)
        get_and_cache = self.cache_aave_utility_tokens(redis_utility_tokens, aave_utility_tokens_data)
        return get_and_cache

def main(version):

    load_dotenv()
    NETWORK = network.show_active()
    ENV_VARS = config["networks"][NETWORK]

    storage_account_name = "dadaiastorage"
    aave_utility_table_name = "aaveUtilityTokens"

    azure_table_client = TableAPI(storage_account_name, DefaultAzureCredential())
    redis_client = RedisAPI(host='redis', port=6379)
    redis_erc20_tokens_key = f"aave_tokens_{NETWORK}_V{version}"
    redis_erc20_tokens = redis_client.get_key(redis_erc20_tokens_key)
    if len(redis_erc20_tokens) == 0: 
        logging.info(f"Chave {redis_erc20_tokens_key} está vazia!")
        return
    
    redis_utility_tokens_key = f"aave_utility_{NETWORK}_V{version}"

    if version == '2':
        aave_contract_addresses_provider = ENV_VARS['aave_v2_addresses_provider']
        aave_obj = AaveV2API(addresses_provider=aave_contract_addresses_provider, network=NETWORK)

    elif version == '3':
        aave_contract_addresses_provider = ENV_VARS['aave_v3_addresses_provider']
        aave_obj = AaveV3API(addresses_provider=aave_contract_addresses_provider, network=NETWORK)
       
    else:
        raise Exception("Versão inválida")
    
    aave_utility_tokens = AaveUtilityTokens(NETWORK, version, aave_obj)
    aave_utility_tokens.configure_services(azure_table_client, aave_utility_table_name, redis_client, redis_utility_tokens_key)

    res = aave_utility_tokens.run(redis_erc20_tokens)
    print(res)
    
