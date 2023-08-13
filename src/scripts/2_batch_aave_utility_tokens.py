import os
from dotenv import load_dotenv
from brownie import interface, config, network
from azure.core.credentials import AzureNamedKeyCredential
import pandas as pd
from scripts.apis.table_storage_api import TableAPI
from scripts.apis.redis_api import RedisAPI
from scripts.apis.aave_apis import get_aave_pool, get_indexes_datatypes
from scripts.apis.erc20_apis import get_ERC20_metadata


class AaveUtilityTokens:

    def __init__(self, azure_table_client, azure_table_name, redis_client, version):
        self.aave_contract = get_aave_pool(version)
        self.azure_table_client = azure_table_client
        self.azure_table_name = azure_table_name
        self.azure_table_client.create_table(azure_table_name)
        self.redis_client = redis_client
        self.list_type_tokens = ["aTokenAddress", "stableDebtTokenAddress", "variableDebtTokenAddress"]
        self.utility_tokens_indexes = get_indexes_datatypes(version, self.list_type_tokens)
        self.version = version


    def __schema_table(self, row, father_token):
        return {
            "PartitionKey": network.show_active(), 
            "RowKey": str(row["tokenAddress"]),
            "FatherToken": str(father_token),
            "version": f"v{self.version}",
            "name": row["name"],
            "symbol": row["symbol"],
            "decimals": int(row["decimals"])
        }


    def get_reserve_tokens(self, token):
        aave_contract = get_aave_pool(self.version)
        list_type_tokens = ["aTokenAddress", "stableDebtTokenAddress", "variableDebtTokenAddress"]
        utility_tokens_indexes = get_indexes_datatypes(self.version, list_type_tokens)
        tokens = [aave_contract.getReserveData(token)[utility_tokens_indexes[type_token]] for type_token in list_type_tokens]
        return {"tokenAddress": token, **{list_type_tokens[i]: tokens[i] for i in range(len(tokens))}}
    

    def __get_list_utility_tokens(self, data_erc20_tokens):
        list_erc20_tokens = list(map(lambda x: x["RowKey"], data_erc20_tokens))
        reserve_tokens = [self.get_reserve_tokens(token) for token in list_erc20_tokens]
        get_utility_tokens = lambda x: (x['tokenAddress'], x["aTokenAddress"], x["stableDebtTokenAddress"], x["variableDebtTokenAddress"])
        list_utility_tokens = list(map(get_utility_tokens, reserve_tokens))
        return list_utility_tokens


    def __get_diff_tokens(self, stored_data, aave_tokens):
        stored_aave_tokens = map(lambda x: x["RowKey"], stored_data)
        diff_tokens = list(set(aave_tokens) - set(stored_aave_tokens))
        return diff_tokens
    

    def __get_azure_table_data(self, ):
        query = f"PartitionKey eq '{network.show_active()}' and version eq 'v{self.version}'"
        return self.azure_table_client.query_table(table=self.azure_table_name, query=query)
    

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
            print(token_data)


    def get_from_azure_and_cache(self, redis_key):
        data_azure_table = self.__get_azure_table_data()
        self.redis_client.register_key(redis_key, data_azure_table)


    def run(self, aave_erc20_key, aave_utility_key, aave_utility_table_name):
        redis_erc20_tokens = self.redis_client.get_key(aave_erc20_key)
        redis_utility_tokens = self.redis_client.get_key(aave_utility_key)
        if len(redis_erc20_tokens) == 0: return f"Chave {aave_erc20_key} não existe"
        aave_utility_tokens_data = self.__get_list_utility_tokens(redis_erc20_tokens)
        aave_utility_tokens = [(token[0], i) for token in aave_utility_tokens_data for i in token[1:]]
        utility_tokens = [j for i, j in aave_utility_tokens]
        redis_missing_tokens = self.__get_diff_tokens(redis_utility_tokens, utility_tokens)
        if len(redis_missing_tokens) > 0:
            data_azure_table = self.__get_azure_table_data()
            azure_missing_tokens = self.__get_diff_tokens(data_azure_table, utility_tokens)
            if len(azure_missing_tokens) == 0:
                self.get_from_azure_and_cache(aave_utility_key)
                return f"Tabela Azure completa e cache atualizado"
            else:
                aave_utility_tokens = list(filter(lambda x: x[1] in azure_missing_tokens, aave_utility_tokens))
                self.__fulfill_azure_table(aave_utility_tokens)
                self.get_from_azure_and_cache(aave_utility_key)
                return f"Tabela azure preenchida e cache atualizado"
        return f"Tabela Azure cacheada"


def main(version):

    load_dotenv()
    storage_account_name = "dadaiastorage"
    aave_erc20_table_name = "aaveERC20Tokens"
    aave_utility_table_name = "aaveUtilityTokens"
    aave_erc20_key = f"aave_tokens_{network.show_active()}_V{version}"
    aave_utility_key = f"aave_utility_{network.show_active()}_V{version}"

    credential = AzureNamedKeyCredential(storage_account_name, os.getenv("STORAGE_KEY"))
    azure_table_client = TableAPI(storage_account_name, credential)
    redis_client = RedisAPI(host='redis', port=6379)

    aave_utility_tokens = AaveUtilityTokens(azure_table_client, aave_utility_table_name, redis_client, version)
    res = aave_utility_tokens.run(aave_erc20_key, aave_utility_key, aave_utility_table_name)
    print(res)
    
