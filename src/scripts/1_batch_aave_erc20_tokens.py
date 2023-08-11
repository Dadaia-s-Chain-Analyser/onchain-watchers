import os
from brownie import network
from dotenv import load_dotenv
from azure.core.credentials import AzureNamedKeyCredential
from scripts.apis.table_storage_api import TableAPI
from scripts.apis.redis_api import RedisAPI
from scripts.apis.aave_apis import get_aave_pool
from scripts.apis.erc20_apis import get_ERC20_metadata


class AaveERC20Tokens:

    def __init__(self, redis_client, azure_table_client, version):
        self.redis_client = redis_client
        self.azure_table_client = azure_table_client
        self.version = version
        self.table_name = "aaveERC20Tokens"
        self.redis_cached_table = f"aave_tokens_{network.show_active()}_V{version}"


    def schema_table(self, row):
        return {
            "PartitionKey": network.show_active(), 
            "RowKey": str(row["tokenAddress"]), 
            "version": f"v{self.version}",
            "name": row["name"],
            "symbol": row["symbol"],
            "decimals": int(row["decimals"])
        }
    

    def get_azure_table_data(self):
        query = f"PartitionKey eq '{network.show_active()}'"
        return self.azure_table_client.query_table(self.table_name, query=query)


    def fulfill_azure_table(self, aave_tokens):
        for token in aave_tokens:
            token_data = get_ERC20_metadata(token)
            data = self.schema_table(token_data)
            self.azure_table_client.insert_entity(self.table_name, data)


    def get_diff_tokens(self, stored_data, aave_tokens):
        stored_aave_tokens = map(lambda x: x["RowKey"], stored_data)
        diff_tokens = list(set(aave_tokens) - set(stored_aave_tokens))
        return diff_tokens
    

    def get_metadata_erc20_tokens(self, aave_tokens):
        table_data_redis = self.redis_client.get_key(self.redis_cached_table)
        redis_missing_tokens = self.get_diff_tokens(table_data_redis, aave_tokens)
        if len(redis_missing_tokens) > 0:
            data_azure_table = self.get_azure_table_data()
            azure_missing_tokens = self.get_diff_tokens(data_azure_table, redis_missing_tokens)
            if azure_missing_tokens == []:
                self.redis_client.register_key(self.redis_cached_table, data_azure_table)
                return f"Tabela Azure completa e cache atualizado"
            else:
                self.fulfill_azure_table(azure_missing_tokens)
                table_data_azure = self.get_azure_table_data()
                self.redis_client.register_key(self.redis_cached_table, table_data_azure)
            return f"Tabela azure preenchida e cache atualizado"
        return f"Tabela Azure cacheada"


def main(version):
    load_dotenv()
    storage_account_name = "dadaiastorage"
    table_name = "aaveERC20Tokens"
    credential = AzureNamedKeyCredential(storage_account_name, os.getenv("STORAGE_KEY"))
    azure_table_client = TableAPI(storage_account_name, credential)
    redis_client = RedisAPI(host='redis', port=6379)
    aave_erc20_obj = AaveERC20Tokens(redis_client, azure_table_client, version)

    aave_contract = get_aave_pool(version)
    aave_tokens = aave_contract.getReservesList()
    azure_table_client.create_table(table_name)
    res = aave_erc20_obj.get_metadata_erc20_tokens(aave_tokens)
    print(res)