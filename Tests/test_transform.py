"""
test_transform.py

Testes do módulo de transformação: join e cálculo das agregações
(total, quantidade, ticket médio) por cliente e por produto.
"""

from transform import build_balanco_produtos, build_resumo_clientes, join_vendas_clientes


def _criar_clientes_df(spark):
    return spark.createDataFrame(
        [(1, "João Silva"), (2, "Maria Souza")],
        ["cliente_id", "nome"],
    )


def _criar_vendas_df(spark):
    return spark.createDataFrame(
        [
            (1, 1, 10001, 480.0),
            (2, 1, 10001, 480.0),
            (3, 1, 10001, 490.0),
            (4, 1, 10002, 500.0),
            (5, 1, 10002, 500.0),
            (6, 2, 10002, 300.0),
        ],
        ["venda_id", "cliente_id", "produto_id", "valor"],
    )


def test_join_mantem_todas_as_vendas_mesmo_sem_cliente_correspondente(spark):
    clientes_df = _criar_clientes_df(spark)
    vendas_df = spark.createDataFrame(
        [(1, 1, 10001, 100.0), (2, 999, 10001, 200.0)],  # cliente_id 999 não existe
        ["venda_id", "cliente_id", "produto_id", "valor"],
    )

    joined_df = join_vendas_clientes(vendas_df, clientes_df)

    # left join: as 2 vendas continuam presentes, mesmo a do cliente inexistente
    assert joined_df.count() == 2


def test_resumo_clientes_calcula_total_quantidade_e_ticket_medio(spark):
    clientes_df = _criar_clientes_df(spark)
    vendas_df = _criar_vendas_df(spark)

    joined_df = join_vendas_clientes(vendas_df, clientes_df)
    resumo_df = build_resumo_clientes(joined_df)

    cliente_1 = resumo_df.filter(resumo_df.cliente_id == 1).collect()[0]

    # 480 + 480 + 490 + 500 + 500 = 2450, em 5 vendas -> ticket médio 490.0
    assert cliente_1["total_vendas"] == 2450.0
    assert cliente_1["quantidade_vendas"] == 5
    assert cliente_1["ticket_medio"] == 490.0


def test_balanco_produtos_calcula_metricas_por_produto(spark):
    vendas_df = _criar_vendas_df(spark)

    balanco_df = build_balanco_produtos(vendas_df)
    produto_10001 = balanco_df.filter(balanco_df.produto_id == 10001).collect()[0]

    # 480 + 480 + 490 = 1450, em 3 vendas -> ticket médio 483.33
    assert produto_10001["total_vendas_produto"] == 1450.0
    assert produto_10001["quantidade_vendas_produto"] == 3
    assert produto_10001["ticket_medio_produto"] == 483.33