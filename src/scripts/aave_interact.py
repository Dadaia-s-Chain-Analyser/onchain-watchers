

class AaveInteract:


    def configure_services(self, azure_table_client, azure_table_name, redis_client, redis_cached_table):
        self.azure_table_client = azure_table_client
        self.azure_table_name = azure_table_name
        self.redis_client = redis_client
        self.redis_cached_table = redis_cached_table


    def _get_azure_table_data(self):
        query = f"PartitionKey eq '{self.network}_aave_v{self.version}'"
        return self.azure_table_client.query_table(table=self.azure_table_name, query=query)
    

    def get_from_azure_and_cache(self):
        data_azure_table = self._get_azure_table_data()
        self.redis_client.register_key(self.redis_cached_table, data_azure_table)

    def _get_diff_tokens(self, stored_data, aave_tokens):
        stored_aave_tokens = map(lambda x: x["RowKey"], stored_data)
        diff_tokens = list(set(aave_tokens) - set(stored_aave_tokens))
        return diff_tokens
    
