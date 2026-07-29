"""
conftest.py

Fixtures compartilhadas entre os testes. O pytest carrega este arquivo
automaticamente (não precisa importar em cada teste).
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession

# permite importar os módulos de src/ nos arquivos de teste
sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
)

# garante que o Spark usa o MESMO interpretador Python que está rodando os
# testes (evita que o Spark tente abrir um "python" genérico do sistema,
# que no Windows pode cair no atalho fantasma da Microsoft Store)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark():
    """
    Cria uma única SparkSession para toda a sessão de testes (mais rápido
    do que criar uma nova em cada teste, já que subir a JVM tem custo alto).
    """
    spark = (
        SparkSession.builder.appName("TestesETL")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()