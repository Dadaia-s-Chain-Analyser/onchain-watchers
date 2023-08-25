from abc import ABC, abstractmethod
from brownie import interface, network, config
from scripts.apis.aave_datatypes import reserves_struct_v2, reserves_struct_v3


class AaveAPI(ABC):

    @abstractmethod
    def get_aave_pool_contract(self):
        raise NotImplementedError
    
    @abstractmethod
    def get_price_oracle_contract(self):
        raise NotImplementedError

    @abstractmethod
    def get_indexes_datatypes(self, list_type_tokens):
        raise NotImplementedError

    @abstractmethod
    def get_protocol_provider(self, **kwargs):
        raise NotImplementedError

class AaveV2API(AaveAPI):

    def __init__(self, addresses_provider, network):
        self.network = network
        self.addresses_provider = interface.IPoolAddressesProviderV2(addresses_provider)

    def get_aave_pool_contract(self):
        pool_address = self.addresses_provider.getLendingPool()
        return interface.IPoolV2(pool_address)
    
    def get_price_oracle_contract(self):
        price_oracle_address = self.addresses_provider.getPriceOracle()
        return interface.IAaveOracleV2(price_oracle_address)

    def get_indexes_datatypes(self, list_type_tokens):
        return {i: reserves_struct_v2.index(list(filter(lambda x: x["campo"] == i, reserves_struct_v2))[0]) for i in list_type_tokens}

    def get_protocol_provider(self, **kwargs):
        try: address = kwargs['protocol_data_provider']
        except KeyError:
            print("Protocol Data Provider não encontrado")
            return
        aave_protocol_provider = interface.IProtocolDataProvider(address)
        return aave_protocol_provider
    

class AaveV3API(AaveAPI):

    def __init__(self, addresses_provider, network):
        self.network = network
        self.addresses_provider = interface.IPoolAddressesProviderV3(addresses_provider)

    def get_aave_pool_contract(self):
        pool_address = self.addresses_provider.getPool()
        return interface.IPoolV3(pool_address)
    
    def get_price_oracle_contract(self):
        price_oracle_address = self.addresses_provider.getPriceOracle()
        return interface.IAaveOracleV3(price_oracle_address)

    def get_indexes_datatypes(self, list_type_tokens):
        return {i: reserves_struct_v3.index(list(filter(lambda x: x["campo"] == i, reserves_struct_v3))[0]) for i in list_type_tokens}
    

    def get_protocol_provider(self, **kwargs):
        aave_protocol_provider = interface.IProtocolDataProvider(self.addresses_provider.getProtocolDataProvider())
        return aave_protocol_provider


if __name__ == '__main__':

    aave_v2_address_provider = config["networks"][network.show_active()]['aave_v2_addresses_provider']
    aave_v2_api = AaveV2API(addresses_provider=aave_v2_address_provider)
