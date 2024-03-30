from abc import ABC, abstractmethod
import json
from brownie import interface, network, config
from scripts.dm_utilities.interfaces import AaveAddressesProvider, AaveDataProvider



class AaveV2AddressProvider(AaveAddressesProvider):

  def __init__(self, addresses_provider, network):
    self.network = network
    self.version = 2
    self.addresses_provider = interface.IPoolAddressesProviderV2(addresses_provider)

  def get_pool_contract(self):
    pool_address = self.addresses_provider.getLendingPool()
    print(pool_address)
    return interface.IPoolV2(pool_address)

  def get_price_oracle_contract(self):
    price_oracle_address = self.addresses_provider.getPriceOracle()
    return interface.IAaveOracleV2(price_oracle_address)



class AaveV2DataProvider(AaveDataProvider):

  def __init__(self, data_provider_addr, network):
    self.network = network
    self.version = 2
    self.data_provider_contract = interface.IDataProviderV2(data_provider_addr)

  def get_all_tokens(self):
    return self.data_provider_contract.getAllATokens()
  
  def get_all_reserve_tokens(self):
    return self.data_provider_contract.getAllReservesTokens()
  
  def get_reserve_configuration_data(self, token):
    return self.data_provider_contract.getReserveConfigurationData(token)
  
  def get_user_reserve_data(self, token, user):
    return self.data_provider_contract.getUserReserveData(token, user)
  
  def get_reserve_token_addresses(self, token):
    return self.data_provider_contract.getReserveTokensAddresses(token)



if __name__ == '__main__':
  pass