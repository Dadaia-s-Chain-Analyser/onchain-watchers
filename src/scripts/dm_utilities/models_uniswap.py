from brownie import interface
import sys


class UniswapFactory:

    def __init__(self, factory_addr, network, version):
        self.network = network
        self.version = version
        self.uni_factory = interface.IUniswapV2Factory(factory_addr)

    def get_uniswap_pair(self, token0, token1):
        return self.uni_factory.getPair(token0, token1)
