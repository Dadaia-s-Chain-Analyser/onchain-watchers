from brownie import interface, network, config
from scripts.apis.aave_datatypes import reserves_struct_v2, reserves_struct_v3


def get_aave_pool(version):
    active_config = config["networks"][network.show_active()]
    if version == '2':
        address = active_config['lendingPoolAddressProvider']
        pool_addresses_provider = interface.IPoolAddressesProviderV2(address)
        pool_address = pool_addresses_provider.getLendingPool()
        lending_pool = interface.IPoolV2(pool_address)
    else:
        address = active_config['poolAdressesProvider']
        pool_addresses_provider = interface.IPoolAddressesProviderV3(address)
        pool_address = pool_addresses_provider.getPool()
        lending_pool = interface.IPoolV3(pool_address)
    return lending_pool


def get_price_oracle(version):
    if version == '2':
        address = config["networks"][network.show_active()]['lendingPoolAddressProvider']
        pool_addresses_provider = interface.IPoolAddressesProviderV2(address)
        price_oracle_address = pool_addresses_provider.getPriceOracle()
        price_oracle = interface.IAaveOracleV2(price_oracle_address)
    else:
        address = config["networks"][network.show_active()]['poolAddressProvider']
        pool_addresses_provider = interface.IPoolAddressesProviderV3(address)
        price_oracle_address = pool_addresses_provider.getPriceOracle()
        price_oracle = interface.IAaveOracleV3(price_oracle_address)
    return price_oracle



def get_indexes_datatypes(version, list_type_tokens):
    reserve = reserves_struct_v2 if version == '2' else reserves_struct_v3
    return {i: reserve.index(list(filter(lambda x: x["campo"] == i, reserve))[0]) for i in list_type_tokens}



def get_protocol_provider():
    address = config["networks"][network.show_active()]['protocol_data_provider']
    aave_protocol_provider = interface.IProtocolDataeProvider(address)
    return aave_protocol_provider