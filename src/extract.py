"""
extract.py

Módulo responsável pela extração (leitura) dos dados de entrada:
- clientes.csv  -> arquivo CSV padrão
- vendas.txt    -> arquivo posicional de largura fixa

Cada função retorna um DataFrame do PySpark já com os tipos corretos.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, substring, to_date
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)

# Tamanho total esperado de cada linha do vendas.txt
# venda_id(5) + cliente_id(5) + produto_id(5) + valor(8) + data_venda(8) = 31
VENDAS_LINE_LENGTH = 31


def get_spark_session(app_name: str = "DesafioETL") -> SparkSession:
    """Cria (ou reaproveita) uma SparkSession."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def read_clientes(spark: SparkSession, path: str) -> DataFrame:
    """
    Lê o arquivo clientes.csv com schema explícito.

    Parâmetros
    ----------
    spark : SparkSession
    path  : caminho do arquivo clientes.csv

    Retorna
    -------
    DataFrame com colunas: cliente_id (int), nome (string), data_nascimento (date)
    """
    schema = StructType(
        [
            StructField("cliente_id", StringType(), nullable=False),
            StructField("nome", StringType(), nullable=False),
            StructField("data_nascimento", StringType(), nullable=True),
        ]
    )

    try:
        df = spark.read.csv(
            path,
            header=True,
            schema=schema,
            sep=",",
            encoding="UTF-8",
        )
    except Exception as e:
        logger.error(f"Erro ao ler o arquivo de clientes em '{path}': {e}")
        raise

    df = (
        df.withColumn("cliente_id", col("cliente_id").cast("int"))
        .withColumn("data_nascimento", to_date(col("data_nascimento"), "yyyy-MM-dd"))
    )

    total = df.count()
    if total == 0:
        logger.warning(f"Nenhum registro encontrado em '{path}'.")
    else:
        logger.info(f"{total} clientes carregados de '{path}'.")

    return df


def read_vendas(spark: SparkSession, path: str) -> DataFrame:
    """
    Lê o arquivo vendas.txt (formato posicional de largura fixa) e
    extrai os campos manualmente com base nas posições do layout:

        venda_id    -> posições 1-5   (5 caracteres)
        cliente_id  -> posições 6-10  (5 caracteres)
        produto_id  -> posições 11-15 (5 caracteres)
        valor       -> posições 16-23 (8 caracteres, 2 casas decimais implícitas)
        data_venda  -> posições 24-31 (8 caracteres, formato YYYYMMDD)

    Parâmetros
    ----------
    spark : SparkSession
    path  : caminho do arquivo vendas.txt

    Retorna
    -------
    DataFrame com colunas:
        venda_id (int), cliente_id (int), produto_id (int),
        valor (double), data_venda (date)

    Linhas com tamanho diferente de 31 caracteres são descartadas e
    contabilizadas em log (não interrompem o pipeline).
    """
    try:
        raw_df = spark.read.text(path)
    except Exception as e:
        logger.error(f"Erro ao ler o arquivo de vendas em '{path}': {e}")
        raise

    total_linhas = raw_df.count()

    # separa linhas válidas (31 chars) das inválidas, sem derrubar o pipeline
    linhas_validas = raw_df.filter(
        col("value").isNotNull() & (col("value") != "")
    ).filter(
        # length() não está importado para manter o exemplo enxuto;
        # usamos substring/rlike como validação alternativa
        col("value").rlike(f"^.{{{VENDAS_LINE_LENGTH}}}$")
    )

    linhas_invalidas = total_linhas - linhas_validas.count()
    if linhas_invalidas > 0:
        logger.warning(
            f"{linhas_invalidas} linha(s) de '{path}' com tamanho inválido "
            f"(esperado {VENDAS_LINE_LENGTH} caracteres) foram descartadas."
        )

    df = linhas_validas.select(
        substring(col("value"), 1, 5).alias("venda_id"),
        substring(col("value"), 6, 5).alias("cliente_id"),
        substring(col("value"), 11, 5).alias("produto_id"),
        substring(col("value"), 16, 8).alias("valor_raw"),
        substring(col("value"), 24, 8).alias("data_venda_raw"),
    )

    df = (
        df.withColumn("venda_id", col("venda_id").cast("int"))
        .withColumn("cliente_id", col("cliente_id").cast("int"))
        .withColumn("produto_id", col("produto_id").cast("int"))
        # valor vem como inteiro representando centavos -> dividir por 100
        .withColumn("valor", (col("valor_raw").cast("long") / 100.0))
        .withColumn("data_venda", to_date(col("data_venda_raw"), "yyyyMMdd"))
        .drop("valor_raw", "data_venda_raw")
    )

    total = df.count()
    if total == 0:
        logger.warning(f"Nenhuma venda válida encontrada em '{path}'.")
    else:
        logger.info(f"{total} vendas carregadas de '{path}'.")

    return df

if __name__ == "__main__":
    # execução isolada do módulo, útil para testes manuais rápidos
    logging.basicConfig(level=logging.INFO)
    spark = get_spark_session()

    clientes_df = read_clientes(spark, "data/clientes.csv")
    vendas_df = read_vendas(spark, "data/vendas.txt")

    clientes_df.show()
    vendas_df.show()

    spark.stop()