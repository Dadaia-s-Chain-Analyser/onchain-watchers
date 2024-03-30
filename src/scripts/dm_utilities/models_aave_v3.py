from abc import ABC, abstractmethod
import json
from brownie import interface, network, config
from scripts.dm_utilities.interfaces import AaveAddressesProvider, AaveDataProvider




class AaveV3AddressProvider(AaveAddressesProvider):

  def __init__(self, addresses_provider, network):
    self.network = network
    self.version = 2
    self.addresses_provider = interface.IPoolAddressesProviderV3(addresses_provider)

  def get_pool_contract(self):
    pool_address = self.addresses_provider.getPool()
    return interface.IPoolV3(pool_address)

  def get_price_oracle_contract(self):
    price_oracle_address = self.addresses_provider.getPriceOracle()
    return interface.IAaveOracleV3(price_oracle_address)



class AaveV3DataProvider(AaveDataProvider):

  def __init__(self, data_provider_addr, network):
    self.network = network
    self.version = 3
    self.addresses_provider = interface.IDataProviderV2(data_provider_addr)

  def get_all_tokens(self):
    return self.addresses_provider.getAllATokens()
  
  def get_all_reserve_tokens(self):
    return self.addresses_provider.getAllReservesTokens()
  
  def get_reserve_configuration_data(self, token):
    return self.addresses_provider.getReserveConfigurationData(token)
  
  def get_user_reserve_data(self, token, user):
    return self.addresses_provider.getUserReserveData(token, user)
  
  def get_reserve_token_addresses(self, token):
    return self.addresses_provider.getReserveTokensAddresses(token)