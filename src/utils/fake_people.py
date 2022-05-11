from utils.data_source import *
from utils.utils import gen_date, gen_distinct, gen_cpf
from utils.data_source import bancos
from unstructured_producers import soma


def gen_nome():
    return f'{gen_distinct(pessoas["nomes"]).lower()} {gen_distinct(pessoas["sobrenomes"]).lower()}'


def gen_client(banco):
    return {
        "cpf": gen_cpf(),
        "nome": gen_nome(),
        "data_inicio": gen_date(bancos[banco]["product"]["data_inicio"], "%d-%m-%Y")}
            
def gen_clients(size, banco):
    return [{**gen_client(banco)} for i in range(size)]
        
        
if __name__ == '__main__':
    soma()