"""
test_extract.py

Testes do módulo de extração: parsing do CSV de clientes e do TXT
posicional de vendas.
"""

import pytest

from extract import read_clientes, read_vendas


def test_read_clientes_retorna_colunas_e_tipos_corretos(spark, tmp_path):
    csv_path = tmp_path / "clientes.csv"
    csv_path.write_text(
        "cliente_id,nome,data_nascimento\n"
        "1,João Silva,1980-05-12\n"
        "2,Maria Souza,1995-07-30\n",
        encoding="utf-8",
    )

    df = read_clientes(spark, str(csv_path))
    rows = df.orderBy("cliente_id").collect()

    assert df.count() == 2
    assert rows[0]["cliente_id"] == 1
    assert rows[0]["nome"] == "João Silva"
    assert str(rows[0]["data_nascimento"]) == "1980-05-12"


def test_read_vendas_faz_parsing_correto_dos_campos_posicionais(spark, tmp_path):
    # venda_id=00001, cliente_id=00001, produto_id=10001, valor=480.00, data=2023-04-01
    txt_path = tmp_path / "vendas.txt"
    txt_path.write_text("0000100001100010004800020230401\n", encoding="utf-8")

    df = read_vendas(spark, str(txt_path))
    row = df.collect()[0]

    assert df.count() == 1
    assert row["venda_id"] == 1
    assert row["cliente_id"] == 1
    assert row["produto_id"] == 10001
    assert row["valor"] == 480.0
    assert str(row["data_venda"]) == "2023-04-01"


def test_read_vendas_descarta_linhas_com_tamanho_invalido(spark, tmp_path):
    txt_path = tmp_path / "vendas_com_erro.txt"
    txt_path.write_text(
        "0000100001100010004800020230401\n"  # linha válida (31 chars)
        "linha_quebrada_muito_curta\n"        # linha inválida
        "0000200001100010004900020230402\n",  # linha válida (31 chars)
        encoding="utf-8",
    )

    df = read_vendas(spark, str(txt_path))

    # apenas as 2 linhas válidas devem ser carregadas; a quebrada é descartada
    assert df.count() == 2


def test_read_clientes_arquivo_inexistente_gera_erro(spark):
    with pytest.raises(Exception):
        read_clientes(spark, "caminho/que/nao/existe.csv").collect()