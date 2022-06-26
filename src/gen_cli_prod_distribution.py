import sys, logging
from utils import get_mysql_engine, handle_parms
from rand_engine.core_batch import create_table
import pandas as pd
from functools import reduce


def gen_client_products_distribution(size):
    distinct_powered = lambda a, b: [False for i in range(a)] + [True for i in range(b)]
    products_metadata = {
        "conta_corrente": dict(method='fake_discrete', distinct=distinct_powered(0, 1)),
        "poupanca": dict(method='fake_discrete', distinct=distinct_powered(3, 2)),
        "seguros": dict(method='fake_discrete', distinct=distinct_powered(4, 1)),
        "consorcio": dict(method='fake_discrete', distinct=distinct_powered(5, 1)),
        "titulos": dict(method='fake_discrete', distinct=distinct_powered(20, 1)),
        "renda_fixa": dict(method='fake_discrete', distinct=distinct_powered(10, 1)),
        "renda_variavel": dict(method='fake_discrete', distinct=distinct_powered(15, 1)),
        "derivativos": dict(method='fake_discrete', distinct=distinct_powered(20, 1)),
    }
    return create_table(size, products_metadata)
    
def gen_products_distribution(database, input_table, output_table):
    db_engine = get_mysql_engine(database)
    df_clients = pd.read_sql(f'SELECT hash_key FROM {input_table}', con=db_engine)
    df_clients_part_2 = gen_client_products_distribution(df_clients.shape[0])
    df = pd.merge(left=df_clients, right=df_clients_part_2, left_on=df_clients.index.values, right_on=df_clients_part_2.index.values, how='left').drop(columns=['key_0'])
    df.to_sql(output_table, if_exists='replace', con=db_engine, index=False)
    return df


if __name__ == '__main__':
    parms = {parm.split("=")[0]: parm.split("=")[1]  
                        for parm in sys.argv if len(parm.split("=")) > 1}
    method_args = dict(database=parms["database"], input_table=parms["input_table"], output_table=parms["output_table"])
    handle_parms(
                    parms, 
                    required_parms=('database', 'input_table', 'output_table'), 
                    method=gen_products_distribution, 
                    method_args=method_args
    )
