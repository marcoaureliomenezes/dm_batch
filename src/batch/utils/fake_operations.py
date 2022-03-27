from random import randint
from utils.utils import *
from utils.data_source import bancos

tickers = ["CAB91", "GAA77", "PETR4"]
banks = bancos.keys()

################################################################################################################

def gen_investment_base(tipo="stock", banco="sant"):
    return {
        "operação": f"{banco}-{tipo}-{gen_str_num(10)}",
        "banco": bancos[banco]["name"],
        "volume": gen_value(1, 1000, chance=0.2, factor=10),
        "risco": gen_value(0, 100),
        "valor": gen_money(1, 1000, chance=0.35, factor=10),
    }

#######################################    INVESTMENTS    #####################################################

def gen_stock(initial_offer=False, **kwargs):
    start_date = kwargs["start_date"] if kwargs.get("start_date") else "2021-01-01"
    data_inicio = gen_date_oper(initial_offer, start_date=start_date)
    return {
        **gen_investment_base(),
        "ticker": f"{gen_distinct(tickers)}",
        "data_inicio": data_inicio,
    }

def gen_investment(initial_offer=False, tipo="CDB", **kwargs):

    start_date = kwargs["start_date"] if kwargs.get("start_date") else "2021-01-01"
    data_inicio = gen_date_oper(initial_offer, start_date=start_date)
    data_vencimento = gen_diff_day(initial_offer, diff_day=100, formato="%Y-%m-%d", start_date=data_inicio)
    
    return {
        **gen_investment_base(tipo=tipo),
        "descrição": f"{tipo} Investment",
        "tipo": tipo,
        "profit": 0,
        "data_inicio": data_inicio,
        "data_vencimento": data_vencimento,
    }
    
    
def gen_investment(initial_offer=False, tipo="investfound-01", **kwargs):
    start_date = kwargs["start_date"] if kwargs.get("start_date") else "2021-01-01"
    data_inicio = gen_date_oper(initial_offer, start_date=start_date)
    return {
        **gen_investment_base(),
        "fundo": tipo,
        "tipo": tipo,
        "data_inicio": data_inicio,
    }    

################################################################################################################

def get_creditCard(initial_offer=True, **kwargs):
    start_date = kwargs["start_date"] if kwargs.get("start_date") else "2021-01-01"
    data_operacao = gen_date_oper(initial_offer, start_date=start_date)                       
    transactions = lambda: {
        "tipo": gen_distinct(["reembolso", "compra"]),
        "descrição": "",
        "valor": gen_money(1, 1000, chance=0.35, factor=10),
        "data_operação": data_operacao
    }
    
    return {
        "limite": gen_money(100, 1000, chance=0.2, factor=10),
        "operações": [transactions() for _ in range(randint(2, 5))],
        "credito_rotativo": gen_money(100, 1000, chance=0.2, factor=10),
        "taxa_credito_rotativo": randint(1, 10) / 100,
    }
    
################################################################################################################

def get_checkings(initial_offer=True, **kwargs):
    start_date = kwargs["start_date"] if kwargs.get("start_date") else "2021-01-01"
    data_operacao = gen_date_oper(initial_offer, start_date=start_date)
    transactions = lambda: {
        "tipo": gen_distinct(["credit", "debit"]),
        "descrição": "",
        "valor": gen_money(1, 100, chance=0.35, factor=10),
        "data_operação": data_operacao
    }
    return {
       "limite": gen_money(10, 5000, chance=0.2, factor=10),
       "balanço": gen_value(100, 10**6),
       "operações": [transactions() for _ in range(randint(2, 5))],
    }
    
    
def get_pix_history(initial_offer=True, **kwargs):
    start_date = kwargs["start_date"] if kwargs.get("start_date") else "2021-01-01"
    data_operacao = gen_date_oper(initial_offer, start_date=start_date)
    oper = lambda: {
        "banco": gen_distinct(banks),
        "agencia": gen_str_num(4),
        "numero_conta": gen_str_num(6),
        "cpf": gen_cpf(),
    }

    return {
        "destinatario": oper(),
        "descrição": "",
        "valor": gen_money(1, 10**4),
        "data_operação": data_operacao
    }


def get_bill(initial_offer=False, **kwargs):
    start_date = kwargs["start_date"] if kwargs.get("start_date") else "2021-01-01"
    data_inicio = gen_date_oper(initial_offer, start_date=start_date)
    data_vencimento = gen_diff_day(initial_offer, diff_day=10, formato="%Y-%m-%d", start_date=data_inicio)
    bank = gen_distinct(banks)
    return {
        "operacao": f"{gen_str_num(10)}-{gen_str_num(2)}",
        "valor": gen_money(1, 10**4),
        "data_inicio": data_inicio,
        "data_vencimento": data_vencimento, 
    }


if __name__ == '__main__':

    print("\n\nAÇÕES")
    res1 = gen_stock(initial_offer=False)
    res2 = gen_stock(initial_offer=True, start_date="2017-01-01")
    print(res1)
    print(res2)
    
    print("\n\nINVESTMENTS TIPO CDB")
    res3 = gen_investment(initial_offer=False, tipo="CDB")
    res4 = gen_investment(initial_offer=True, tipo="CDB", start_date="2017-01-01")
    print(res3)
    print(res4)
    
    print("\n\nINVESTMENTS TIPO LCA")
    res5 = gen_investment(initial_offer=False, tipo="LCA")
    res6 = gen_investment(initial_offer=True, tipo="LCA", start_date="2017-01-01")
    print(res5)
    print(res6)
    
    print("\n\nINVESTMENTS TIPO LCI")
    res7 = gen_investment(initial_offer=False, tipo="LCI")
    res8 = gen_investment(initial_offer=True, tipo="LCI", start_date="2017-01-01")
    print(res7)
    print(res8)
    
    print("\n\nINVESTMENTS TIPO CRI")
    res9 = gen_investment(initial_offer=False, tipo="CRI")
    res10 = gen_investment(initial_offer=True, tipo="CRI", start_date="2017-01-01")
    print(res9)
    print(res10)
    
    print("\n\nINVESTMENTS TIPO CRA")
    res11 = gen_investment(initial_offer=False, tipo="CRA")
    res12 = gen_investment(initial_offer=True, tipo="CRA", start_date="2017-01-01")
    print(res11)
    print(res12)
   
    print("\n\nCREDIT_CARD")
    res13 = get_creditCard(initial_offer=False)
    res14 = get_creditCard(initial_offer=True, start_date="2017-01-01")
    print(res13) 
    print(res14)
    
    print("\n\nCHECKINGS")
    res15 = get_checkings(initial_offer=False)
    res16 = get_checkings(initial_offer=True, start_date="2017-01-01")
    print(res15) 
    print(res16)
    
    print("\n\nPIX")
    res17 = get_pix_history(initial_offer=False)
    res18 = get_pix_history(initial_offer=True, start_date="2017-01-01")
    print(res17)
    print(res18)
    
    print("\n\nBILLS")
    res19 = get_bill(initial_offer=False)
    res20 = get_bill(initial_offer=True, start_date="2017-01-01")
    print(res19)
    print(res20)
