from yahooquery import Ticker
import pandas as pd
import json, argparse
from sqlalchemy import create_engine


def get_assets(tipo="commodities"):
    with open('/app/data/assets_dict.json') as json_file:
        data = json.load(json_file)
    return data[tipo]


def get_ticker_data(ticker, period="max"):
    emp_ticker = Ticker(ticker["symbol"])
    result = emp_ticker.history(period=period)
    columns = ['symbol', 'date', 'high', 'open', 'close', 'volume', 'low', 'adjclose']
    if type(result) == dict:
        return pd.DataFrame(columns=columns)
    result = result.reset_index()
    result["name"] = ticker["name"]
    return result[columns]


def get_assets_group(conn_dict, table_name, period):
    assets_info = get_assets(table_name)
    user, password, service, db = [conn_dict.get(key) for key in ["user", "password", "host", "db"]]
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{service}/{db}')
    for asset in assets_info:
        
        df_symbol = get_ticker_data(asset, period)
        df_symbol.to_sql(table_name, con = engine,if_exists='append', index=False)

    return 0


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='python arguments')
    params = ['--user', '--password', '--host', '--port', '--db', '--period']
    _ = [parser.add_argument(param, help='', required=True) for param in params]

    args = parser.parse_args()
    parm_user, parm_password, parm_host, parm_port, parm_db, parm_period = \
            (args.user, args.password, args.host, int(args.port), args.db, args.period)

    conn_dict = {
    "user": parm_user,
    "password":parm_password, 
    "host": parm_host,
    "port": parm_port, 
    "db": parm_db
    }
    asset_group = ["markets", "big_techs", "ibovespa", "commodities", "cryptocoins", "stablecoins"]
    [get_assets_group(conn_dict, asset, parm_period) for asset in asset_group]
