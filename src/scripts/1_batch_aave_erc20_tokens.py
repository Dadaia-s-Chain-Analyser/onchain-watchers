import logging
import pandas as pd
from brownie import network
from scripts.utils.utils import setup_database, table_exists, find_missing_keys
from scripts.utils.interfaces import get_aave_pool, get_ERC20_metadata

logging.basicConfig(level='INFO')


def main(version):
    db_engine = setup_database()
    erc20_table = "erc_20_tokens"
    aave_contract = get_aave_pool(version)
    aave_tokens = aave_contract.getReservesList()
    if table_exists(db_engine, erc20_table):
        aave_tokens = find_missing_keys(aave_tokens, db_engine, erc20_table, pivot='tokenAddress')
        if len(aave_tokens) == 0:
            logging.info(f"Information about tokens AAVE V{version} already updated")
            return
    description = f"AAVE V{version}" if network.show_active() != 'mainnet' else ""
    df_erc20_data = pd.DataFrame([get_ERC20_metadata(token, description) for token in aave_tokens])
    df_erc20_data.to_sql(erc20_table, con=db_engine, if_exists='append', index=False)
