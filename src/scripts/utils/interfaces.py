import sys
from requests import HTTPError
from brownie import network, config, interface
from scripts.aave_datatypes import reserves_struct_v2, reserves_struct_v3


ACTIVE_NETWORK = config["networks"][network.show_active()]



def get_ERC20_contract(token_address):
    erc20_contract = interface.IERC20(token_address)
    return erc20_contract


def get_ERC20_metadata(token, description):
    ERC20_contract = interface.IERC20(token)
    res = dict(tokenAddress = token, decimals = ERC20_contract.decimals())
    try:
        res['name'] = ERC20_contract.name()
        res['symbol'] = ERC20_contract.symbol()
    except OverflowError as e:
        print(f"Error with token {token}")
        res['name'] = None
        res['symbol']  = None
    res['description'] = description
    return res

    
def get_V3_aggregator(oracle_contract, asset_address):
    try:
        aggregator_address = oracle_contract.getSourceOfAsset(asset_address)
        null_address = '0x0000000000000000000000000000000000000000'
        aggregator_address = aggregator_address if aggregator_address != null_address else ACTIVE_NETWORK['ether_pricefeed']
    except HTTPError as e:
        if str(e)[:3] == '429':
            sys.exit(13)
        print(e)

    return interface.AggregatorV3Interface(aggregator_address)

def get_protocol_provider():
    address = ACTIVE_NETWORK['protocol_data_provider']
    aave_protocol_provider = interface.IProtocolDataeProvider(address)
    return aave_protocol_provider




def get_uniswap_factory(version=2):
    name_uniswap_contract = f'uniswapV{version}Factory'
    try:
        address_uniswap_factory = ACTIVE_NETWORK[name_uniswap_contract]
        uniswap_contract = interface.IUniswapV2Factory(address_uniswap_factory)
    except KeyError as e: 
        print(f'{name_uniswap_contract} address not found on network!')
        sys.exit(15)
    else:
        return uniswap_contract


def get_uniswap_pair_pool(pool_address):
    return interface.IUniswapV2Pair(pool_address)


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

    
def get_indexes_datatypes(version, list_type_tokens):
    reserve = reserves_struct_v2 if version == '2' else reserves_struct_v3
    return {i: reserve.index(list(filter(lambda x: x["campo"] == i, reserve))[0]) for i in list_type_tokens}


def get_price_oracle(version):
    if version == '2':
        address = ACTIVE_NETWORK['lendingPoolAddressProvider']
        pool_addresses_provider = interface.IPoolAddressesProviderV2(address)
        price_oracle_address = pool_addresses_provider.getPriceOracle()
        price_oracle = interface.IAaveOracleV2(price_oracle_address)
    else:
        address = ACTIVE_NETWORK['poolAddressProvider']
        pool_addresses_provider = interface.IPoolAddressesProviderV3(address)
        price_oracle_address = pool_addresses_provider.getPriceOracle()
        price_oracle = interface.IAaveOracleV3(price_oracle_address)
    return price_oracle


def main(version):
    res = get_indexes_datatypes("aTokenAddress")
    print(res)