from utils.fake_people import gen_clients
from sqlalchemy import create_engine
import pandas as pd
import os, argparse
from utils import *


parser = argparse.ArgumentParser(description='python arguments')
parser.add_argument('--user', help='', required=True)
parser.add_argument('--password', help= '', required=True)
parser.add_argument('--host', help= '', required=True)
parser.add_argument('--port', help= '', required=True)
parser.add_argument('--db', help= '', required=True)
parser.add_argument('--size', help= '', required=True)
parser.add_argument('--bank', help= '', required=True)

args = parser.parse_args()
parm_user = args.user
parm_password = args.password
parm_host = args.host
parm_port = int(args.port)
parm_db = args.db
parm_size = int(args.size)
parm_bank = args.bank

def populate_clients(size, bank, conn_dict, table_name):
    user, password, service = [conn_dict.get(key) for key in ["user", "password", "host"]]
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{service}/data_master')
    
    clients = gen_clients(size, bank)
    df_clients = pd.DataFrame(clients)
    df_clients.to_sql(table_name, con = engine,if_exists='append')
    return

if __name__ == '__main__':

    conn_dict = {
    "user": parm_user,
    "password":parm_password, 
    "host": parm_host, 
    "port": parm_port, 
    "db": parm_db
    }
    
    populate_clients(parm_size, parm_bank, conn_dict, "clients")