from brownie import interface

class ERC20API:

    def _get_ERC20_contract(self, address):
        return interface.IERC20(address)
    

    def get_ERC20_metadata(self, token_address):
        ERC20_contract = self._get_ERC20_contract(token_address)
        res = {
            'tokenAddress': token_address,
            'decimals': ERC20_contract.decimals()
        }
        try:
            res['name'] = ERC20_contract.name()
            res['symbol'] = ERC20_contract.symbol()
        except OverflowError:
            print(f"Error with token {token_address}")
            res['name'] = None
            res['symbol']  = None
        return res
    
    def get_total_supply(self):
        ERC20_contract = self._get_ERC20_contract()
        return ERC20_contract.totalSupply()

    def get_balance(self, address):
        ERC20_contract = self._get_ERC20_contract()
        return ERC20_contract.balanceOf(address)
