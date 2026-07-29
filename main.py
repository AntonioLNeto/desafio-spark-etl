import argparse
import logging
import os
import sys

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
    parser = argparse.ArgumentParser(description="Pipeline ETL: integra clientes e vendas, gera resumos.")
    parser.add_argument("--clientes", default="data/clientes.csv")
    parser.add_argument("--vendas", default="data/vendas.txt")
    parser.add_argument("--output", default="output")
    parser.add_argument("--formato", default="csv", choices=["csv", "parquet"])
    return parser.parse_args()


def validar_arquivos_entrada(clientes_path: str, vendas_path: str) -> None:
    for path, nome in [(clientes_path, "clientes"), (vendas_path, "vendas")]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Arquivo de {nome} não encontrado em '{path}'.")
        if os.path.getsize(path) == 0:
            raise ValueError(f"Arquivo de {nome} em '{path}' está vazio.")


def main() -> int:
    args = parse_args()

    try:
        validar_arquivos_entrada(args.clientes, args.vendas)
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Falha na validação dos arquivos de entrada: {e}")
        return 1

    spark = None
    try:
        spark = get_spark_session()

        try:
            clientes_df = read_clientes(spark, args.clientes)
            vendas_df = read_vendas(spark, args.vendas)
        except Exception as e:
            logger.error(f"Falha na etapa de extração: {e}")
            return 1

        if clientes_df.count() == 0:
            logger.error("Nenhum cliente foi carregado. Abortando pipeline.")
            return 1
        if vendas_df.count() == 0:
            logger.error("Nenhuma venda válida foi carregada. Abortando pipeline.")
            return 1

        try:
            joined_df = join_vendas_clientes(vendas_df, clientes_df)
            resumo_clientes_df = build_resumo_clientes(joined_df)
            balanco_produtos_df = build_balanco_produtos(vendas_df)
        except Exception as e:
            logger.error(f"Falha na etapa de transformação: {e}")
            return 1

        try:
            save_resumo_clientes(resumo_clientes_df, args.output, formato=args.formato)
            save_balanco_produtos(balanco_produtos_df, args.output, formato=args.formato)
        except Exception as e:
            logger.error(f"Falha na etapa de carga: {e}")
            return 1

        logger.info("Pipeline executado com sucesso.")
        return 0

    except Exception as e:
        logger.error(f"Erro inesperado durante a execução do pipeline: {e}")
        return 1

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())