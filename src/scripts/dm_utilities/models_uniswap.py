from brownie import config, interface
import sys


class UniswapAPI:

    def __init__(self, network, version):
        self.network = network
        self.version = version
        self.uni_factory = self.get_uniswap_factory()

    def get_uniswap_factory(self):
        name_uniswap_contract = f'uniswapV{self.version}Factory'
        try:
            address_uniswap_factory = config["networks"][self.network][name_uniswap_contract]
            uniswap_contract = interface.IUniswapV2Factory(address_uniswap_factory)
        except KeyError as e: 
            print(f'{name_uniswap_contract} address not found on network!')
            sys.exit(15)
        else:
            return uniswap_contract

    def get_uniswap_pair(self, token0, token1):
        return self.uni_factory.getPair(token0, token1)
