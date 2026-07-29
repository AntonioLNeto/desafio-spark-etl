import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from extract import read_clientes, read_vendas
from transform import build_balanco_produtos, build_resumo_clientes, join_vendas_clientes
from load import save_balanco_produtos, save_resumo_clientes, write_dataframe


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.appName("TestesETL")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def _clientes_df(spark):
    return spark.createDataFrame(
        [(1, "João Silva"), (2, "Maria Souza")],
        ["cliente_id", "nome"],
    )


def _vendas_df(spark):
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


# extract

def test_read_clientes(spark, tmp_path):
    csv_path = tmp_path / "clientes.csv"
    csv_path.write_text(
        "cliente_id,nome,data_nascimento\n"
        "1,João Silva,1980-05-12\n"
        "2,Maria Souza,1995-07-30\n",
        encoding="utf-8",
    )

    df = read_clientes(spark, str(csv_path))
    row = df.orderBy("cliente_id").collect()[0]

    assert df.count() == 2
    assert row["cliente_id"] == 1
    assert row["nome"] == "João Silva"
    assert str(row["data_nascimento"]) == "1980-05-12"


def test_read_vendas_parsing_posicional(spark, tmp_path):
    txt_path = tmp_path / "vendas.txt"
    txt_path.write_text("0000100001100010004800020230401\n", encoding="utf-8")

    df = read_vendas(spark, str(txt_path))
    row = df.collect()[0]

    assert row["venda_id"] == 1
    assert row["cliente_id"] == 1
    assert row["produto_id"] == 10001
    assert row["valor"] == 480.0
    assert str(row["data_venda"]) == "2023-04-01"


def test_read_vendas_ignora_linha_invalida(spark, tmp_path):
    txt_path = tmp_path / "vendas.txt"
    txt_path.write_text(
        "0000100001100010004800020230401\n"
        "linha_quebrada\n"
        "0000200001100010004900020230402\n",
        encoding="utf-8",
    )

    df = read_vendas(spark, str(txt_path))
    assert df.count() == 2


def test_read_clientes_arquivo_inexistente(spark):
    with pytest.raises(Exception):
        read_clientes(spark, "nao/existe.csv").collect()


# transform

def test_join_mantem_vendas_sem_cliente(spark):
    clientes_df = _clientes_df(spark)
    vendas_df = spark.createDataFrame(
        [(1, 1, 10001, 100.0), (2, 999, 10001, 200.0)],
        ["venda_id", "cliente_id", "produto_id", "valor"],
    )

    joined_df = join_vendas_clientes(vendas_df, clientes_df)
    assert joined_df.count() == 2


def test_resumo_clientes(spark):
    joined_df = join_vendas_clientes(_vendas_df(spark), _clientes_df(spark))
    resumo_df = build_resumo_clientes(joined_df)
    cliente_1 = resumo_df.filter(resumo_df.cliente_id == 1).collect()[0]

    assert cliente_1["total_vendas"] == 2450.0
    assert cliente_1["quantidade_vendas"] == 5
    assert cliente_1["ticket_medio"] == 490.0


def test_balanco_produtos(spark):
    balanco_df = build_balanco_produtos(_vendas_df(spark))
    produto = balanco_df.filter(balanco_df.produto_id == 10001).collect()[0]

    assert produto["total_vendas_produto"] == 1450.0
    assert produto["quantidade_vendas_produto"] == 3
    assert produto["ticket_medio_produto"] == 483.33


# load

def test_write_dataframe_formato_invalido(spark, tmp_path):
    df = spark.createDataFrame([(1, "teste")], ["id", "nome"])
    with pytest.raises(ValueError):
        write_dataframe(df, str(tmp_path / "saida"), formato="xlsx")


def test_save_resumo_clientes(spark, tmp_path):
    resumo_df = spark.createDataFrame(
        [(1, "João Silva", 2450.0, 5, 490.0)],
        ["cliente_id", "nome", "total_vendas", "quantidade_vendas", "ticket_medio"],
    )
    save_resumo_clientes(resumo_df, str(tmp_path), formato="csv")

    lido_df = spark.read.csv(str(tmp_path / "resumo_clientes"), header=True, inferSchema=True)
    row = lido_df.collect()[0]

    assert lido_df.count() == 1
    assert row["cliente_id"] == 1
    assert row["total_vendas"] == 2450.0


def test_save_balanco_produtos(spark, tmp_path):
    balanco_df = spark.createDataFrame(
        [(10001, 1450.0, 3, 483.33)],
        ["produto_id", "total_vendas_produto", "quantidade_vendas_produto", "ticket_medio_produto"],
    )
    save_balanco_produtos(balanco_df, str(tmp_path), formato="csv")

    lido_df = spark.read.csv(str(tmp_path / "balanco_produtos"), header=True, inferSchema=True)
    assert lido_df.count() == 1
    assert lido_df.collect()[0]["produto_id"] == 10001