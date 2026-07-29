"""
test_load.py

Testes do módulo de carga: verifica se os dados são escritos em disco
corretamente e se formatos inválidos são rejeitados.
"""

import pytest

from load import save_balanco_produtos, save_resumo_clientes, write_dataframe


def test_write_dataframe_formato_invalido_gera_erro(spark, tmp_path):
    df = spark.createDataFrame([(1, "teste")], ["id", "nome"])

    with pytest.raises(ValueError):
        write_dataframe(df, str(tmp_path / "saida"), formato="xlsx")


def test_save_resumo_clientes_grava_arquivo_legivel(spark, tmp_path):
    resumo_df = spark.createDataFrame(
        [(1, "João Silva", 2450.0, 5, 490.0)],
        ["cliente_id", "nome", "total_vendas", "quantidade_vendas", "ticket_medio"],
    )

    save_resumo_clientes(resumo_df, str(tmp_path), formato="csv")

    # relê o que foi salvo para confirmar que o conteúdo bate
    lido_df = spark.read.csv(
        str(tmp_path / "resumo_clientes"), header=True, inferSchema=True
    )
    row = lido_df.collect()[0]

    assert lido_df.count() == 1
    assert row["cliente_id"] == 1
    assert row["total_vendas"] == 2450.0


def test_save_balanco_produtos_grava_arquivo_legivel(spark, tmp_path):
    balanco_df = spark.createDataFrame(
        [(10001, 1450.0, 3, 483.33)],
        [
            "produto_id",
            "total_vendas_produto",
            "quantidade_vendas_produto",
            "ticket_medio_produto",
        ],
    )

    save_balanco_produtos(balanco_df, str(tmp_path), formato="csv")

    lido_df = spark.read.csv(
        str(tmp_path / "balanco_produtos"), header=True, inferSchema=True
    )

    assert lido_df.count() == 1
    assert lido_df.collect()[0]["produto_id"] == 10001