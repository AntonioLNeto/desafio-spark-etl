# Desafio_spark — Pipeline ETL com PySpark

Pipeline de ETL (Extract, Transform, Load) que integra dados de clientes e
vendas, calcula métricas financeiras e gera relatórios estruturados por
cliente e por produto.

## Objetivo

Consolidar dados de duas fontes diferentes (um CSV de clientes e um TXT
posicional de vendas), calcular o total de vendas, quantidade de vendas e
ticket médio, e entregar dois arquivos parquet, sendo o segundo particionada pela data da venda:

- resumo_clientes —  cliente_id, nome, total_vendas, quantidade_vendas, ticket_medio
- balanco_produtos —   produto_id, total_vendas_produto, quantidade_vendas_produto, ticket_medio_produto

## Estrutura do projeto

```
Case spark/
├── data/
│   ├── clientes.csv
│   └── vendas.txt
├── src/
│   ├── extract.py       # leitura do CSV e do TXT posicional
│   ├── transform.py     # join e cálculo das métricas
│   └── load.py          # escrita dos outputs
├── output/
│   ├── resumo_clientes.parquet
│   └── balanco_produtos.parquet
├── main.py               # ponto de entrada do pipeline
├── requirements.txt
└── README.md
```

## Pré-requisitos

- Python 3.9+
- Java 8 ou 11 (necessário para o PySpark)
- PySpark
- pytest (para os testes)

## Instalação

```bash
# criar e ativar um ambiente virtual
python -m venv venv
venv\Scripts\activate      # Windows
source venv/bin/activate   # Linux/Mac

# instalar dependências
pip install -r requirements.txt
```

`requirements.txt`:
```
pyspark==3.5.1
pytest==8.2.0
```

## Como rodar o pipeline

```bash
python main.py --clientes data/clientes.csv --vendas data/vendas.txt --output output/
```

Parâmetros:
| Parâmetro     | Descrição                                  | Padrão        |
|---------------|---------------------------------------------|---------------|
| `--clientes`  | Caminho do arquivo clientes.csv              | `data/clientes.csv` |
| `--vendas`    | Caminho do arquivo vendas.txt (posicional)   | `data/vendas.txt`   |
| `--output`    | Pasta onde os resultados serão salvos        | `output/`     |

## Formato dos dados de entrada

**clientes.csv**
```
cliente_id,nome,data_nascimento
1,João Silva,1980-05-12
2,Maria Souza,1995-07-30
```

**vendas.txt** (arquivo posicional, largura fixa de 31 caracteres por linha)

| Campo       | Posições | Tamanho |
|-------------|----------|---------|
| venda_id    | 1–5      | 5       |
| cliente_id  | 6–10     | 5       |
| produto_id  | 11–15    | 5       |
| valor       | 16–23    | 8 (2 casas decimais implícitas) |
| data_venda  | 24–31    | 8 (YYYYMMDD) |

Exemplo de linha:
```
0000100001100010004800020230401
```
(venda 00001, cliente 00001, produto 10001, valor 480.00, data 2023-04-01)

## Formato dos dados de saída

**resumo_clientes.csv**
| cliente_id | nome       | total_vendas | quantidade_vendas | ticket_medio |
|------------|------------|--------------|--------------------|--------------|
| 1          | João Silva | 2450.00      | 5                  | 490.00       |

**balanco_produtos.csv**
| produto_id | total_vendas_produto | quantidade_vendas_produto | ticket_medio_produto |
|------------|------------------------|-----------------------------|------------------------|
| 10001      | 1450.00                | 3                            | 483.33                 |

## Testes

```bash
pytest tests/
```

Os testes cobrem:
- parsing correto dos campos posicionais do vendas.txt
- cálculo das agregações (total, quantidade, ticket médio)
- comportamento do pipeline com arquivos ausentes ou linhas mal formatadas

## Tratamento de erros

- Validação de existência dos arquivos de entrada antes da leitura
- Linhas do vendas.txt com tamanho diferente de 31 caracteres são
  registradas em log e ignoradas, sem interromper o pipeline
- Vendas sem cliente correspondente são reportadas separadamente

## Observações

- A leitura do vendas.txt é feita manualmente (via `substring`), respeitando
  as posições fixas de cada campo, conforme especificado no desafio
- Particionamento por `data_venda` nos outputs pode ser habilitado como
  diferencial (ver flag `--partition-by-date` em `main.py`)
- Projeto testado em ambiente local, sem dependência de infraestrutura em nuvem
