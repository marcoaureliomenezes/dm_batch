import sys, logging
from utils import get_mysql_engine, handle_parms
from rand_engine.core_batch import create_table
import pandas as pd
from functools import reduce

logging.basicConfig(level=logging.INFO)


def gen_conta_corrente(size):
    conta_corrente_metadata = {
        "saldo": dict(method='fake_floats', min=-2000, max=10000, round=2),
        "limite": dict(method="fake_discrete", distinct=[100 * i for i in range(50)]),
    }
    return create_table(size, conta_corrente_metadata)


def gen_poupanca(size):
    poupanca_metadata = {
        "saldo": dict(method='fake_floats', min=0, max=500),
    }
    return create_table(size, poupanca_metadata)


def gen_seguros(size):
    distinct_powered = lambda a, b: [False for i in range(a)] + [True for i in range(b)]
    seguro_metadata = {
        "tipo_seguro": dict(method="fake_discrete", distinct=["VEICULAR", "VIDA", "DESASTRES NATURAIS"]),
        "valor_parcela": dict(method="fake_discrete", distinct=[round(100. * i, 2) for i in range(10)]),
        "valor_pago": dict(method="fake_discrete", distinct=[0]),
        "regular": dict(method="fake_discrete", distinct=distinct_powered(1, 12)),
    }
    return create_table(size, seguro_metadata)

def gen_consorcio(size):
    distinct_powered = lambda a, b: [False for i in range(a)] + [True for i in range(b)]
    consorcio_metadata = {
        "tipo_consorcio": dict(method="fake_discrete", distinct=["OURO", "PRATA", "BRONZE"]),
        "valor_parcela": dict(method="fake_discrete", distinct=[round(100. * i, 2) for i in range(2)]),
        "parcelas_pagas": dict(method="fake_discrete", distinct=[0]),
        "premio_recebido": dict(method="fake_discrete", distinct=distinct_powered(1, 12)),
        "data_inicio": dict(method='fake_dates', start="30-6-2021", end="30-6-2022", formato="%d-%m-%Y"),
        "data_vencimento_DU": dict(method='fake_ints', min=0, max=20)
    }
    return create_table(size, consorcio_metadata)

def gen_titulos(size):
    titulos_metadata = {
        "ativo": dict(method="fake_discrete", distinct=["SELIC-PRE","SELIC-POS","IPCA_PRE","IPCA-POS"]),
        "total_investido": dict(method="fake_discrete", distinct=[0]),
        "data_inicio": dict(method='fake_dates', start="30-06-2019", end="30-06-2022", formato="%d-%m-%Y")
    }
    return create_table(size, titulos_metadata)

def gen_renda_fixa(size):
    renda_fixa_metadata = {
        "ativo": dict(method="fake_discrete", distinct=["CDB","LCI","LCA","Fundos"]),
        "total_investido": dict(method="fake_discrete", distinct=[0]),
        "data_inicio": dict(method='fake_dates', start="30-06-2016", end="30-06-2022", formato="%d-%m-%Y")
    }
    return create_table(size, renda_fixa_metadata)

def gen_renda_variavel(size):
    renda_variavel_metadata = {
        "categoria": dict(method="fake_discrete", distinct=["TSLA","MSFT","AAPL","IPCA-POS"]),
        "total_investido": dict(method="fake_discrete", distinct=[0]),
        "data_inicio": dict(method='fake_dates', start="30-06-2020", end="30-06-2022", formato="%d-%m-%Y")
    }
    return create_table(size, renda_variavel_metadata)

def gen_derivativos(size):
    derivativos_metadata = {
    "categoria": dict(method="fake_discrete", distinct=["FUTUROS", "TERMO", "SWAP", "OPCOES"]),
    "tipo": dict(method='fake_discrete', distinct=["COMPRA", "VENDA"]),
    "data_inicio": dict(method='fake_dates', start="30-06-2021", end="30-06-2022", formato="%d-%m-%Y")
    }
    return create_table(size, derivativos_metadata)

def get_client_products_distribution(table, engine):
    return pd.read_sql(f"SELECT * FROM {table}", con=engine)

def populate_client_products_tables(database, input_table, output_table):
    engine =  get_mysql_engine(database)
    df_clients_distribution = get_client_products_distribution(input_table, engine)
    df_part1 = df_clients_distribution[df_clients_distribution[output_table] == True][['hash_key']].reset_index(drop=True)
    df_part2 = globals()[f'gen_{output_table}'](df_part1.shape[0])
    df = pd.merge(left=df_part1, right=df_part2, left_on=df_part1.index.values, right_on=df_part2.index.values, how='left').drop(columns=['key_0'])
    df.to_sql(f'prod_{output_table}', con=engine, index=False, if_exists='replace')
    return df

if __name__ == '__main__':
    parms = {parm.split("=")[0]: parm.split("=")[1]  
                        for parm in sys.argv if len(parm.split("=")) > 1}
    method_args = dict(database=parms["database"], input_table=parms["input_table"], output_table=parms["output_table"])
    handle_parms(
                    parms, 
                    required_parms=('database', 'input_table', 'output_table'), 
                    method=populate_client_products_tables, 
                    method_args=method_args
    )
