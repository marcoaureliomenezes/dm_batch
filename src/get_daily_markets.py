from yahooquery import Ticker
import pandas as pd
import logging, sys
from functools import reduce
from utils import table_exists, get_mysql_engine


logging.basicConfig(level=logging.INFO)


def get_ticker_data(ticker, period="max"):
    emp_ticker = Ticker(ticker)
    result = emp_ticker.history(period=period)
    columns = ['symbol', 'date', 'high', 'open', 'close', 'volume', 'low', 'adjclose']
    if type(result) == dict:
        return pd.DataFrame(columns=columns)
    result = result.reset_index()
    return result[columns]

def remove_duplicates(engine, table):
    if table_exists(engine, table):
        df_cleaning = pd.read_sql(f"SELECT * FROM {table}", con=engine)
        df_cleaning = df_cleaning.drop_duplicates()
        df_cleaning.to_sql(table, con = engine, if_exists='replace', index=False)
    else:
        logging.info("The table doesn't exists")

def get_assets_group(period, asset_group, database, table):
    asset_group = asset_group.split(",")
    engine = get_mysql_engine(database)
    for asset in asset_group:
        df_symbol = get_ticker_data(asset, period)
        df_symbol.to_sql(table, con = engine, if_exists='append', index=False)
    remove_duplicates(engine, table)
    return df_symbol


if __name__ == '__main__':

    parms = {parm.split("=")[0]: parm.split("=")[1]  
                        for parm in sys.argv if len(parm.split("=")) > 1}   
    real_parms = set(parms.keys())
    required_parms = {'period', 'database', 'table', 'asset_group'}

    if real_parms.intersection(required_parms) == required_parms:
        get_assets_group(
            period=parms['period'], 
            asset_group=parms['asset_group'],
            database=parms['database'], 
            table=parms['table'])
    else:
        logging.info(f"Parms ({reduce(lambda a, b: f'{a}, {b}', required_parms)}) are required")
