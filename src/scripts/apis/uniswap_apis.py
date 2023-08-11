from brownie import network, config, interface
import sys

def get_uniswap_factory(version=2):
    name_uniswap_contract = f'uniswapV{version}Factory'
    try:
        address_uniswap_factory = config["networks"][network.show_active()][name_uniswap_contract]
        uniswap_contract = interface.IUniswapV2Factory(address_uniswap_factory)
    except KeyError as e: 
        print(f'{name_uniswap_contract} address not found on network!')
        sys.exit(15)
    else:
        return uniswap_contract


def get_uniswap_pair_pool(pool_address):
    return interface.IUniswapV2Pair(pool_address)