import hashlib, sys
from utils import get_mysql_engine, handle_parms
from rand_engine.core_batch import create_table
from rand_engine.templates import template_batch, nomes, sobrenomes



def gen_clients_random_data(size):
    clientes = {
        "nome": dict(method="fake_discrete", formato="x x", key="x", 
            params=[
                {'how': "fake_discrete", 'distinct': nomes},
                {'how': "fake_discrete", 'distinct': sobrenomes}
        ]),
        "cpf": template_batch('cpf'),
        "saldo": dict(method='fake_floats', min=0, max=10000),
        "agencia": dict(method='fake_ints', min=0, max=10**4 - 1),
        "conta": dict(method='fake_ints', min=0, max=10**8 - 1),
        "renda_mensal": dict(method='fake_floats', min=0, max=500, round=0, factor=100),
        "senha": dict(method='fake_ints', min=0, max=10**8 - 1),
        "data_nascimento": dict(method='fake_dates', start="01-01-1945", end="31-12-2003", formato="%d-%m-%Y"),
        "data_entrada": dict(method='fake_dates', start="01-01-2010", end="31-12-2020", formato="%d-%m-%Y")
    }
    return create_table(size, clientes)


def generate_clients(size):
    encripting = lambda entrada: hashlib.md5(str(entrada).encode('utf8')).hexdigest()
    hashing = lambda entrada: hashlib.sha256(entrada.encode()).hexdigest()[:16]
    df_clients = gen_clients_random_data(size)
    df_clients["senha"] = df_clients["senha"].apply(encripting)
    df_clients["hash_input"] =  df_clients[['agencia', 'conta', 'cpf']].astype('str').agg(''.join, axis=1)
    df_clients["hash_key"] = df_clients["hash_input"].apply(hashing)
    output_cols = ['hash_key', 'nome', 'cpf', 'agencia', 'conta', 'renda_mensal', 'saldo',
    'senha', 'data_nascimento', 'data_entrada']
    return df_clients[output_cols]


def populate_client_accounts_table(size, database, table):
    df_clients = generate_clients(size)
    engine = get_mysql_engine(database)
    df_clients.to_sql(table, con = engine,if_exists='replace', index=False)
    return df_clients


if __name__ == '__main__':
    parms = {parm.split("=")[0]: parm.split("=")[1]  
                        for parm in sys.argv if len(parm.split("=")) > 1}
    method_args = dict(size=int(parms["size"]), database=parms["database"], table=parms["table"])
    handle_parms(
                    parms, 
                    required_parms=('size', 'database', 'table'), 
                    method=populate_client_accounts_table, 
                    method_args=method_args
    )