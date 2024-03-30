from abc import ABC, abstractmethod


class AaveAddressesProvider(ABC):

  @abstractmethod
  def get_pool_contract(self):
    raise NotImplementedError
    
  @abstractmethod
  def get_price_oracle_contract(self):
    raise NotImplementedError



class AaveDataProvider(ABC):
  
    @abstractmethod
    def get_all_tokens(self):
      raise NotImplementedError
    
    @abstractmethod
    def get_all_reserve_tokens(self):
      raise NotImplementedError
    
    @abstractmethod
    def get_reserve_configuration_data(self, token):
      raise NotImplementedError
    
    @abstractmethod
    def get_user_reserve_data(self, token, user):
      raise NotImplementedError
  
    @abstractmethod
    def get_reserve_token_addresses(self, token):
      raise NotImplementedError


