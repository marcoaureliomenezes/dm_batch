from sqlalchemy import create_engine
import argparse
from rand_engine.core_batch import create_table
from rand_engine.templates import template_batch, nomes, sobrenomes


def generate_clients(size):
    clientes1 = {
                "nomes": dict(method="fake_discrete", formato="x x", key="x", 
                    params=[
                        {'how': "fake_discrete", 'distinct': nomes},
                        {'how': "fake_discrete", 'distinct': sobrenomes}
                ]),
                "cpf": template_batch('cpf'),
                "saldo": dict(method='fake_floats', min=0, max=10000),
                "renda_mensal": dict(method='fake_floats', min=0, max=500, round=0, factor=100),
                "endereco": template_batch('endereco'),
                "data_nascimento": dict(method='fake_dates', start="01-01-1945", end="31-12-2003", formato="%d-%m-%Y"),
                "data_entrada": dict(method='fake_dates', start="01-01-2010", end="31-12-2020", formato="%d-%m-%Y")
        }
    return create_table(size, clientes1)

def populate_clients(size, conn_dict, table_name):
    df_clients = generate_clients(size)
    user, password, service = [conn_dict.get(key) for key in ["user", "password", "host"]]
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{service}/data_master')
    df_clients.to_sql(table_name, con = engine,if_exists='append')
    return df_clients

def generate_stock_market_clients(size, ampl):
    client_stock_market = {
        "client_id": dict(method='fake_floats', min=0, max=ampl),
        "saldo_inicial": dict(method='fake_floats', min=0, max=100, round=0, factor=100),
        "data_entrada": dict(method='fake_dates', start="01-01-2010", end="31-12-2020", formato="%d-%m-%Y")

    }
    return create_table(size, client_stock_market)

def populate_stock_market_clients(size, ampl, conn_dict, table_name):
    df_stock_market_clients = generate_stock_market_clients(size, ampl)
    user, password, service = [conn_dict.get(key) for key in ["user", "password", "host"]]
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{service}/data_master')
    df_stock_market_clients.to_sql(table_name, con = engine,if_exists='append')
    return df_stock_market_clients


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='python arguments')
    params = ['--user', '--password', '--host', '--port', '--db', '--size']
    _ = [parser.add_argument(param, help='', required=True) for param in params]

    args = parser.parse_args()
    parm_user, parm_password, parm_host, parm_port, parm_db, parm_size = \
            (args.user, args.password, args.host, int(args.port), args.db, int(args.size))

    conn_dict = {
    "user": parm_user,
    "password":parm_password, 
    "host": parm_host, 
    "port": parm_port, 
    "db": parm_db
    }
    
    populate_clients(parm_size, conn_dict, "clients")
    populate_stock_market_clients(int(parm_size/10), parm_size, conn_dict, "clients_stock_market")