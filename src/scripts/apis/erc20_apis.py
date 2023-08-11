from brownie import interface


def get_ERC20_contract(token_address):
    erc20_contract = interface.IERC20(token_address)
    return erc20_contract


def get_ERC20_metadata(token):
    ERC20_contract = interface.IERC20(token)
    res = dict(tokenAddress = token, decimals = ERC20_contract.decimals())
    try:
        res['name'] = ERC20_contract.name()
        res['symbol'] = ERC20_contract.symbol()
    except OverflowError as e:
        print(f"Error with token {token}")
        res['name'] = None
        res['symbol']  = None
    return res