from brownie import interface
from requests import HTTPError


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