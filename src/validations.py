"""
validations.py

Módulo de validações de qualidade de dados (data quality checks).

Diferente do tratamento de erros (que lida com arquivos ausentes ou
malformados), aqui o objetivo é detectar dados que são tecnicamente
válidos, mas estatisticamente suspeitos — e apenas ALERTAR sobre eles
(via log), sem interromper o pipeline. A decisão de investigar ou agir
fica a critério de quem analisa o log.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    date_format,
    lag,
    sum as spark_sum,
)
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def detectar_tickets_atipicos(
    vendas_df: DataFrame,
    coluna_valor: str = "valor",
    multiplicador_iqr: float = 1.5,
) -> DataFrame:
    """
    Sinaliza vendas cujo valor é atipicamente alto, usando o método do
    intervalo interquartil (IQR) — o mesmo critério usado em box-plots.

    Por que IQR em vez de média + desvio padrão? Média e desvio padrão são
    facilmente "contaminados" por um único valor muito extremo: um outlier
    gigante infla a própria média e o próprio desvio, fazendo-o parecer
    normal (efeito chamado de "masking"). Mediana e quartis não sofrem
    desse problema, o que torna o IQR mais confiável, principalmente em
    bases pequenas ou com poucos outliers.

    Critério: qualquer valor acima de Q3 + (multiplicador_iqr * IQR) é
    considerado atípico, onde IQR = Q3 - Q1.

    Parâmetros
    ----------
    vendas_df         : DataFrame de vendas (precisa ter a coluna de valor)
    coluna_valor       : nome da coluna com o valor da venda
    multiplicador_iqr  : multiplicador do IQR para definir o limite
                         (padrão: 1.5, o critério clássico de box-plot)

    Retorna
    -------
    DataFrame apenas com as vendas consideradas atípicas (pode ser vazio).
    Também loga um warning com a quantidade encontrada.
    """
    quartis = vendas_df.approxQuantile(coluna_valor, [0.25, 0.75], 0.0)
    q1, q3 = quartis[0], quartis[1]
    iqr = q3 - q1

    if iqr == 0:
        logger.info(
            "IQR igual a zero (dados pouco variados ou muito poucos "
            "registros) — checagem de tickets atípicos pulada."
        )
        return vendas_df.limit(0)

    limite = q3 + (multiplicador_iqr * iqr)

    atipicos_df = vendas_df.filter(col(coluna_valor) > limite)
    qtd_atipicos = atipicos_df.count()

    if qtd_atipicos > 0:
        logger.warning(
            f"{qtd_atipicos} venda(s) com valor atípico detectada(s) "
            f"(acima de {limite:.2f}, critério: Q3={q3:.2f} + "
            f"{multiplicador_iqr}x IQR={iqr:.2f}). Recomenda-se revisão manual."
        )
        for row in atipicos_df.collect():
            logger.warning(
                f"  -> venda_id={row['venda_id']}, cliente_id={row['cliente_id']}, "
                f"valor={row[coluna_valor]:.2f}"
            )
    else:
        logger.info("Nenhuma venda com valor atípico detectada.")

    return atipicos_df


def detectar_variacao_mom(
    vendas_df: DataFrame,
    coluna_data: str = "data_venda",
    coluna_valor: str = "valor",
    limite_percentual: float = 50.0,
) -> DataFrame:
    """
    Calcula a variação percentual do total de vendas mês a mês (MoM —
    month over month) e sinaliza meses cuja variação (para cima ou para
    baixo) ultrapassa o limite definido.

    Parâmetros
    ----------
    vendas_df         : DataFrame de vendas
    coluna_data        : coluna de data da venda (tipo date)
    coluna_valor       : coluna de valor da venda
    limite_percentual  : variação percentual (absoluta) acima da qual o
                         mês é sinalizado (padrão: 50%)

    Retorna
    -------
    DataFrame com colunas: mes, total_vendas, variacao_percentual
    Apenas os meses com variação suspeita são destacados no log; o
    DataFrame retornado contém a série completa para referência.
    """
    vendas_por_mes = (
        vendas_df.withColumn("mes", date_format(col(coluna_data), "yyyy-MM"))
        .groupBy("mes")
        .agg(spark_sum(coluna_valor).alias("total_vendas"))
        .orderBy("mes")
    )

    janela = Window.orderBy("mes")
    variacao_df = vendas_por_mes.withColumn(
        "total_mes_anterior", lag("total_vendas").over(janela)
    ).withColumn(
        "variacao_percentual",
        ((col("total_vendas") - col("total_mes_anterior")) / col("total_mes_anterior")) * 100,
    )

    meses_suspeitos = variacao_df.filter(
        (col("variacao_percentual").isNotNull())
        & ((col("variacao_percentual") > limite_percentual) | (col("variacao_percentual") < -limite_percentual))
    )

    qtd_suspeitos = meses_suspeitos.count()
    if qtd_suspeitos > 0:
        logger.warning(
            f"{qtd_suspeitos} mês(es) com variação de vendas acima de "
            f"{limite_percentual}% em relação ao mês anterior:"
        )
        for row in meses_suspeitos.collect():
            logger.warning(
                f"  -> {row['mes']}: variação de {row['variacao_percentual']:.1f}% "
                f"(total: {row['total_vendas']:.2f}, mês anterior: {row['total_mes_anterior']:.2f})"
            )
    else:
        logger.info(
            f"Nenhum mês com variação acima de {limite_percentual}% detectado."
        )

    return variacao_df.drop("total_mes_anterior")


if __name__ == "__main__":
    # execução isolada do módulo, útil para testes manuais rápidos
    import sys
    import os

    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))
    from extract import get_spark_session, read_vendas

    logging.basicConfig(level=logging.INFO)
    spark = get_spark_session()

    vendas_df = read_vendas(spark, "data/vendas.txt")

    detectar_tickets_atipicos(vendas_df)
    detectar_variacao_mom(vendas_df).show()

    spark.stop()