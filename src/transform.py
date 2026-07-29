"""
transform.py

Módulo responsável pela transformação dos dados:
- join entre vendas e clientes
- cálculo do resumo por cliente (total_vendas, quantidade_vendas, ticket_medio)
- cálculo do balanço por produto (total_vendas_produto, quantidade_vendas_produto,
  ticket_medio_produto)
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, round as spark_round, sum as spark_sum

logger = logging.getLogger(__name__)


def join_vendas_clientes(vendas_df: DataFrame, clientes_df: DataFrame) -> DataFrame:
    """
    Realiza o join entre vendas e clientes usando cliente_id.

    Usa um left join a partir de vendas: toda venda é mantida mesmo que,
    por algum motivo, o cliente_id não exista no cadastro de clientes
    (nesse caso, a coluna 'nome' vem como null). Isso evita perder vendas
    silenciosamente por causa de dados de cadastro incompletos.

    Parâmetros
    ----------
    vendas_df   : DataFrame retornado por extract.read_vendas
    clientes_df : DataFrame retornado por extract.read_clientes

    Retorna
    -------
    DataFrame com todas as colunas de vendas + nome do cliente
    """
    joined_df = vendas_df.join(clientes_df, on="cliente_id", how="left")

    vendas_sem_cliente = joined_df.filter(col("nome").isNull()).count()
    if vendas_sem_cliente > 0:
        logger.warning(
            f"{vendas_sem_cliente} venda(s) referenciam um cliente_id "
            f"que não existe no cadastro de clientes."
        )

    return joined_df


def build_resumo_clientes(joined_df: DataFrame) -> DataFrame:
    """
    Calcula, para cada cliente, o total de vendas, a quantidade de vendas
    e o ticket médio.

    Parâmetros
    ----------
    joined_df : DataFrame já resultante do join (vendas + clientes)

    Retorna
    -------
    DataFrame com colunas:
        cliente_id, nome, total_vendas, quantidade_vendas, ticket_medio
    """
    resumo_df = (
        joined_df.groupBy("cliente_id", "nome")
        .agg(
            spark_round(spark_sum("valor"), 2).alias("total_vendas"),
            count("venda_id").alias("quantidade_vendas"),
            spark_round(avg("valor"), 2).alias("ticket_medio"),
        )
        .orderBy("cliente_id")
    )

    logger.info(f"Resumo gerado para {resumo_df.count()} cliente(s).")
    return resumo_df


def build_balanco_produtos(vendas_df: DataFrame) -> DataFrame:
    """
    Calcula, para cada produto, o total de vendas, a quantidade de vendas
    e o ticket médio.

    Não depende do join com clientes — é calculado diretamente sobre as
    vendas.

    Parâmetros
    ----------
    vendas_df : DataFrame retornado por extract.read_vendas

    Retorna
    -------
    DataFrame com colunas:
        produto_id, total_vendas_produto, quantidade_vendas_produto,
        ticket_medio_produto
    """
    balanco_df = (
        vendas_df.groupBy("produto_id")
        .agg(
            spark_round(spark_sum("valor"), 2).alias("total_vendas_produto"),
            count("venda_id").alias("quantidade_vendas_produto"),
            spark_round(avg("valor"), 2).alias("ticket_medio_produto"),
        )
        .orderBy("produto_id")
    )

    logger.info(f"Balanço gerado para {balanco_df.count()} produto(s).")
    return balanco_df


if __name__ == "__main__":
    # execução isolada do módulo, útil para testes manuais rápidos
    from extract import get_spark_session, read_clientes, read_vendas

    logging.basicConfig(level=logging.INFO)
    spark = get_spark_session()

    clientes_df = read_clientes(spark, "data/clientes.csv")
    vendas_df = read_vendas(spark, "data/vendas.txt")

    joined_df = join_vendas_clientes(vendas_df, clientes_df)

    resumo_clientes_df = build_resumo_clientes(joined_df)
    balanco_produtos_df = build_balanco_produtos(vendas_df)

    resumo_clientes_df.show()
    balanco_produtos_df.show()

    spark.stop()