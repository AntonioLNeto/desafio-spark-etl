"""
load.py

Módulo responsável pela escrita (load) dos resultados do pipeline:
- resumo_clientes
- balanco_produtos

Suporta saída em CSV ou Parquet, e particionamento opcional por data
(diferencial citado no enunciado, aplicado sobre o detalhe de vendas
join com clientes — já que os resumos finais são agregações sem coluna
de data).
"""

import logging
import os

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

FORMATOS_SUPORTADOS = {"csv", "parquet"}


def _validar_formato(formato: str) -> None:
    if formato not in FORMATOS_SUPORTADOS:
        raise ValueError(
            f"Formato '{formato}' não suportado. Use um de: {FORMATOS_SUPORTADOS}"
        )


def write_dataframe(
    df: DataFrame,
    output_path: str,
    formato: str = "csv",
    single_file: bool = True,
) -> None:
    """
    Escreve um DataFrame em disco no formato especificado.

    Parâmetros
    ----------
    df           : DataFrame a ser salvo
    output_path  : caminho de destino (pasta, pois o Spark grava em partes)
    formato      : "csv" ou "parquet"
    single_file  : se True, força a saída em um único arquivo (coalesce(1)).
                   Útil para arquivos pequenos como estes; evite em datasets
                   grandes, pois perde o paralelismo da escrita.
    """
    _validar_formato(formato)

    try:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        writer_df = df.coalesce(1) if single_file else df
        writer = writer_df.write.mode("overwrite")

        if formato == "csv":
            writer.option("header", "true").csv(output_path)
        else:
            writer.parquet(output_path)

        logger.info(f"Dados salvos com sucesso em '{output_path}' (formato={formato}).")

    except Exception as e:
        logger.error(f"Erro ao salvar dados em '{output_path}': {e}")
        raise


def save_resumo_clientes(
    resumo_df: DataFrame, output_dir: str, formato: str = "csv"
) -> None:
    """Salva o resumo por cliente em <output_dir>/resumo_clientes."""
    path = os.path.join(output_dir, "resumo_clientes")
    write_dataframe(resumo_df, path, formato=formato)


def save_balanco_produtos(
    balanco_df: DataFrame, output_dir: str, formato: str = "csv"
) -> None:
    """Salva o balanço por produto em <output_dir>/balanco_produtos."""
    path = os.path.join(output_dir, "balanco_produtos")
    write_dataframe(balanco_df, path, formato=formato)


def save_vendas_detalhado_particionado(
    joined_df: DataFrame, output_dir: str, formato: str = "csv"
) -> None:
    """
    Diferencial opcional do enunciado: salva o detalhe de vendas (já com
    o nome do cliente, resultado do join) particionado por data_venda.

    Diferente dos resumos agregados, esse dataset mantém uma linha por
    venda, então faz sentido particionar por data.

    Gera uma subpasta por data, ex:
        <output_dir>/vendas_detalhado/data_venda=2023-04-01/...
    """
    _validar_formato(formato)
    path = os.path.join(output_dir, "vendas_detalhado")

    try:
        os.makedirs(output_dir, exist_ok=True)
        writer = joined_df.write.mode("overwrite").partitionBy("data_venda")

        if formato == "csv":
            writer.option("header", "true").csv(path)
        else:
            writer.parquet(path)

        logger.info(f"Vendas detalhadas particionadas por data salvas em '{path}'.")

    except Exception as e:
        logger.error(f"Erro ao salvar vendas particionadas em '{path}': {e}")
        raise


if __name__ == "__main__":
    # execução isolada do módulo: roda o pipeline completo (extract -> transform -> load)
    from extract import get_spark_session, read_clientes, read_vendas
    from transform import build_balanco_produtos, build_resumo_clientes, join_vendas_clientes

    logging.basicConfig(level=logging.INFO)
    spark = get_spark_session()

    clientes_df = read_clientes(spark, "data/clientes.csv")
    vendas_df = read_vendas(spark, "data/vendas.txt")

    joined_df = join_vendas_clientes(vendas_df, clientes_df)
    resumo_clientes_df = build_resumo_clientes(joined_df)
    balanco_produtos_df = build_balanco_produtos(vendas_df)

    save_resumo_clientes(resumo_clientes_df, "output", formato="csv")
    save_balanco_produtos(balanco_produtos_df, "output", formato="csv")
    # diferencial (opcional):
    # save_vendas_detalhado_particionado(joined_df, "output", formato="csv")

    spark.stop()