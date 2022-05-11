from rand_engine.core_batch import create_table
from rand_engine.templates import template_batch, nomes, sobrenomes

metadata_1 = {
            "nomes": dict(method="fake_discrete", formato="x x", key="x", 
                params=[
                    {'how': "fake_discrete", 'distinct': nomes},
                    {'how': "fake_discrete", 'distinct': sobrenomes}
            ]),
            "email": template_batch('email'),
            "cpf": template_batch('cpf'),
            "cnpj": template_batch('cnpj'),
            'idade': dict(method='fake_ints', min=0, max=100),
            "saldo": dict(method='fake_floats', min=0, max=100),
            "data_entrada": dict(method='fake_dates', start="01-01-2010", end="31-12-2020", formato="%d-%m-%Y")
    }
table = create_table(10, metadata_1)
print(table)


