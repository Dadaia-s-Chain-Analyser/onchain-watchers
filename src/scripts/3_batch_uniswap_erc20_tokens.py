import logging
import pandas as pd
from brownie import interface, config, network
from scripts.utils.utils import setup_database, table_exists, find_missing_keys
from scripts.utils.interfaces import get_ERC20_metadata

logging.basicConfig(level='INFO')



def main(version):
    db_engine = setup_database()
    erc20_table = "erc_20_tokens"
    network_config = config["networks"][network.show_active()]
    link_token = network_config["link_token"]
    weth_token = network_config["weth_token"]
    uni_token = network_config["uni_token"]
    tokens = [link_token, weth_token, uni_token]

    if table_exists(db_engine, erc20_table):
        tokens = find_missing_keys(tokens, db_engine, erc20_table, pivot="tokenAddress")
        if len(tokens) == 0:
            logging.info(f"Official tokens already updated")
            return
    tokens_info = {token: "OFFICIAL" for token in tokens}
    df_erc20_data = pd.DataFrame([get_ERC20_metadata(token, desc) for token, desc in tokens_info.items()])
    df_erc20_data.to_sql(erc20_table, con=db_engine, if_exists='append', index=False)