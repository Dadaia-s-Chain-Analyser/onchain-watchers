import sys, logging
import pandas as pd
from requests import HTTPError
from brownie import interface, config, network
from scripts.utils.utils import setup_database, table_exists
from scripts.utils.interfaces import get_aave_pool, get_indexes_datatypes


logging.basicConfig(level='INFO')

def get_reserve_tokens(token, version):
    aave_contract = get_aave_pool(version)
    list_type_tokens = ["aTokenAddress", "stableDebtTokenAddress", "variableDebtTokenAddress"]
    utility_tokens_indexes = get_indexes_datatypes(version, list_type_tokens)
    tokens = [aave_contract.getReserveData(token)[utility_tokens_indexes[type_token]] for type_token in list_type_tokens]
    return {"tokenAddress": token, **{list_type_tokens[i]: tokens[i] for i in range(len(tokens))}}


def find_missing_tokens(new_tokens, db_engine, table_name):
    df_new_tokens = pd.DataFrame(new_tokens, columns=['token_address'])
    df_recorded_tokens = pd.read_sql_query(f"SELECT tokenAddress FROM {table_name}", con=db_engine)
    df_tokens = pd.merge(df_new_tokens, df_recorded_tokens, left_on='token_address', right_on='tokenAddress', how='left')
    token_missing = df_tokens.loc[df_tokens['tokenAddress'].isnull(), 'token_address'].values
    return token_missing


def main(version):
    db_engine = setup_database()
    aave_utility_table = "aave_utility_tokens"
    query = f"SELECT tokenAddress FROM erc_20_tokens WHERE description = 'AAVE V{version}'"
    df_erc20_aave = pd.read_sql_query(query, con=db_engine)
    list_utility_aave = df_erc20_aave["tokenAddress"].values
    if table_exists(db_engine, aave_utility_table):
        list_utility_aave = find_missing_tokens(list_utility_aave, db_engine, aave_utility_table)
        if len(list_utility_aave) == 0:
            logging.info(f"Information about tokens AAVE V{version} already updated")
            return
    df_utility_aave = pd.DataFrame([get_reserve_tokens(token, version) for token in list_utility_aave])
    df_utility_aave.to_sql(aave_utility_table, con=db_engine, if_exists='append', index=False)
