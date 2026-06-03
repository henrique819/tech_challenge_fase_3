"""
etl_spark.py — TRANSFORMAÇÃO (Bronze -> Silver) em PySpark
Lê os 3 CSV mensais, seleciona as 20 perguntas + estruturais, recodifica os códigos do
IBGE em rótulos legíveis, deriva faixa etária e mês, e grava a base tratada.

Saída: dados_tratados/pnad_covid.csv  (arquivo único, pronto para carga no BigQuery)
       dados_tratados/pnad_covid.parquet

Nota: para ~1,1 milhão de linhas Spark é overkill — a escolha é por escala (vigilância
contínua) e por demonstrar a competência de Big Data. Roda em local[*]; o mesmo código
escala para um cluster sem mudar a lógica.
"""
from __future__ import annotations
import glob, shutil, pathlib
from pyspark.sql import SparkSession, functions as F

ENTRADA = "dados_brutos/PNAD_COVID_*2020.csv"
OUT = pathlib.Path("dados_tratados"); OUT.mkdir(exist_ok=True)

PERGUNTAS = [
    "A002", "A003", "A004", "A005",
    "B0011", "B0012", "B0013", "B0014", "B0015", "B00111", "B005", "B002",
    "B008", "B011", "B007", "C013",
    "C001", "C007", "C01012", "D0051",
]
ESTRUTURAIS = ["Ano", "V1013", "UF", "CAPITAL", "V1022", "V1032"]
COLUNAS = ESTRUTURAIS + PERGUNTAS

SIM_NAO_VARS = ["B0011", "B0012", "B0013", "B0014", "B0015", "B00111",
                "B005", "B002", "B008", "B007", "C013", "C001", "D0051"]


def recodificar(df):
    def sim_nao(c):
        return (F.when(F.col(c) == 1, "Sim").when(F.col(c) == 2, "Não")
                 .when(F.col(c) == 9, "Ignorado").otherwise(None))
    for v in SIM_NAO_VARS:
        df = df.withColumn(v, sim_nao(v))

    df = df.withColumn("A003", F.when(F.col("A003") == 1, "Homem")
                       .when(F.col("A003") == 2, "Mulher"))
    df = df.withColumn("A004",
        F.when(F.col("A004") == 1, "Branca").when(F.col("A004") == 2, "Preta")
         .when(F.col("A004") == 3, "Amarela").when(F.col("A004") == 4, "Parda")
         .when(F.col("A004") == 5, "Indígena").otherwise("Ignorado"))
    df = df.withColumn("A005",
        F.when(F.col("A005") == 1, "Sem instrução").when(F.col("A005") == 2, "Fundamental incompleto")
         .when(F.col("A005") == 3, "Fundamental completo").when(F.col("A005") == 4, "Médio incompleto")
         .when(F.col("A005") == 5, "Médio completo").when(F.col("A005") == 6, "Superior incompleto")
         .when(F.col("A005") == 7, "Superior completo").when(F.col("A005") == 8, "Pós-graduação"))
    df = df.withColumn("B011",
        F.when(F.col("B011") == 1, "Não fez restrição")
         .when(F.col("B011") == 2, "Reduziu contato, mas saiu")
         .when(F.col("B011") == 3, "Ficou em casa, só saiu p/ básico")
         .when(F.col("B011") == 4, "Ficou rigorosamente isolado"))
    df = df.withColumn("C007",
        F.when(F.col("C007") == 1, "Doméstico").when(F.col("C007") == 2, "Militar")
         .when(F.col("C007") == 3, "Policial/Bombeiro").when(F.col("C007") == 4, "Setor privado")
         .when(F.col("C007") == 5, "Setor público").when(F.col("C007") == 6, "Empregador")
         .when(F.col("C007") == 7, "Conta própria").when(F.col("C007") == 8, "Trab. familiar auxiliar")
         .otherwise("Ignorado"))
    df = df.withColumn("mes_nome",
        F.when(F.col("V1013") == 9, "Setembro").when(F.col("V1013") == 10, "Outubro")
         .when(F.col("V1013") == 11, "Novembro"))
    df = df.withColumn("faixa_etaria",
        F.when(F.col("A002") <= 12, "0-12").when(F.col("A002") <= 19, "13-19")
         .when(F.col("A002") <= 39, "20-39").when(F.col("A002") <= 59, "40-59").otherwise("60+"))

    renome = {
        "V1032": "peso", "A002": "idade", "A003": "sexo", "A004": "raca", "A005": "escolaridade",
        "B0011": "sint_febre", "B0012": "sint_tosse", "B0013": "sint_garganta",
        "B0014": "sint_dificuldade_respirar", "B0015": "sint_dor_cabeca",
        "B00111": "sint_perda_olfato_paladar", "B005": "internado", "B002": "procurou_saude",
        "B008": "fez_teste", "B011": "grau_isolamento", "B007": "tem_plano_saude",
        "C013": "home_office", "C001": "trabalhou", "C007": "posicao_ocupacao",
        "C01012": "rendimento", "D0051": "auxilio_emergencial",  # D0051 = recebeu auxílio emergencial (bloco D)
    }
    for a, n in renome.items():
        df = df.withColumnRenamed(a, n)
    return df


def main() -> None:
    spark = SparkSession.builder.appName("PNAD-COVID-ETL").master("local[*]").getOrCreate()
    print("[1/3] Lendo CSVs com Spark")
    df = spark.read.option("header", True).option("inferSchema", True).csv(ENTRADA)
    df = df.select([c for c in COLUNAS if c in df.columns])
    print(f"      linhas: {df.count():,}")

    print("[2/3] Recodificando (Silver)")
    df = recodificar(df).dropna(subset=["peso"])

    print("[3/3] Gravando base tratada")
    df.write.mode("overwrite").parquet(str(OUT / "pnad_covid.parquet"))
    tmp = OUT / "_tmp_csv"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(tmp))
    parte = glob.glob(str(tmp / "part-*.csv"))[0]
    shutil.move(parte, str(OUT / "pnad_covid.csv"))
    shutil.rmtree(tmp)
    print(f"      -> {OUT / 'pnad_covid.csv'}")
    spark.stop()
    print("ETL concluído.")


if __name__ == "__main__":
    main()
