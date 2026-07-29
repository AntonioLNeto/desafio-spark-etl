"""
main.py

Ponto de entrada do pipeline de ETL. Orquestra extract -> transform -> load,
com parametrização via linha de comando e tratamento de erros centralizado.

Uso:
    python main.py --clientes data/clientes.csv --vendas data/vendas.txt --output output/
    python main.py  (usa os caminhos padrão: data/clientes.csv, data/vendas.txt, output/)
"""

import argparse
import logging
import os
import sys

# permite importar os módulos de src/ mesmo rodando main.py a partir da raiz
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from extract import get_spark_session, read_clientes, read_vendas
from load import save_balanco_produtos, save_resumo_clientes
from transform import build_balanco_produtos, build_resumo_clientes, join_vendas_clientes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline ETL: integra clientes e vendas, gera resumos."
    )
    parser.add_argument(
        "--clientes",
        default="data/clientes.csv",
        help="Caminho do arquivo clientes.csv (padrão: data/clientes.csv)",
    )
    parser.add_argument(
        "--vendas",
        default="data/vendas.txt",
        help="Caminho do arquivo vendas.txt posicional (padrão: data/vendas.txt)",
    )
    parser.add_argument(
        "--output",
        default="output",
        help="Pasta de destino dos resultados (padrão: output/)",
    )
    parser.add_argument(
        "--formato",
        default="csv",
        choices=["csv", "parquet"],
        help="Formato de saída dos arquivos (padrão: csv)",
    )
    return parser.parse_args()


def validar_arquivos_entrada(clientes_path: str, vendas_path: str) -> None:
    """
    Valida que os arquivos de entrada existem e não estão vazios,
    antes de sequer inicializar o Spark. Falha rápido e com mensagem clara.
    """
    for path, nome in [(clientes_path, "clientes"), (vendas_path, "vendas")]:
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"Arquivo de {nome} não encontrado em '{path}'. "
                f"Verifique o caminho informado em --{nome}."
            )
        if os.path.getsize(path) == 0:
            raise ValueError(f"Arquivo de {nome} em '{path}' está vazio.")


def main() -> int:
    args = parse_args()

    # ---- 1. Validação de entrada (antes de iniciar o Spark) ----
    try:
        validar_arquivos_entrada(args.clientes, args.vendas)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Falha na validação dos arquivos de entrada: {e}")
        return 1

    spark = None
    try:
        spark = get_spark_session()

        # ---- 2. Extract ----
        try:
            clientes_df = read_clientes(spark, args.clientes)
            vendas_df = read_vendas(spark, args.vendas)
        except Exception as e:
            logger.error(f"Falha na etapa de extração (extract): {e}")
            return 1

        if clientes_df.count() == 0:
            logger.error("Nenhum cliente foi carregado. Abortando pipeline.")
            return 1
        if vendas_df.count() == 0:
            logger.error("Nenhuma venda válida foi carregada. Abortando pipeline.")
            return 1

        # ---- 3. Transform ----
        try:
            joined_df = join_vendas_clientes(vendas_df, clientes_df)
            resumo_clientes_df = build_resumo_clientes(joined_df)
            balanco_produtos_df = build_balanco_produtos(vendas_df)
        except Exception as e:
            logger.error(f"Falha na etapa de transformação (transform): {e}")
            return 1

        # ---- 4. Load ----
        try:
            save_resumo_clientes(resumo_clientes_df, args.output, formato=args.formato)
            save_balanco_produtos(balanco_produtos_df, args.output, formato=args.formato)
        except Exception as e:
            logger.error(f"Falha na etapa de carga (load): {e}")
            return 1

        logger.info("Pipeline executado com sucesso.")
        return 0

    except Exception as e:
        # rede de segurança para qualquer erro não previsto (ex: falha do Spark)
        logger.error(f"Erro inesperado durante a execução do pipeline: {e}")
        return 1

    finally:
        # garante que a SparkSession é encerrada mesmo se algo falhar no meio
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())