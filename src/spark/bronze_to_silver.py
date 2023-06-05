from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import month, regexp_replace, year

from src.settings import SPARK_CONNECT_ENDPOINT, DatalakeZones

BRONZE_DATA_URI = DatalakeZones.BRONZE.to_s3_uri(
    spark_fmt=True,
    prefix="vacinacao_covid/covid_vacinacao_sp.csv",
)
SILVER_DATA_URI = DatalakeZones.SILVER.to_s3_uri(
    spark_fmt=True,
    prefix="vacinacao_covid",
)

spark: SparkSession = (
    SparkSession.builder.appName("Bronze to Silver")  # type: ignore
    .remote(SPARK_CONNECT_ENDPOINT)
    .getOrCreate()
)

df: DataFrame = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("inferSchema", "true")
    .load(BRONZE_DATA_URI)
)

df = df.drop("vacina_fabricante_referencia")
df = df.dropna(
    subset=[
        "paciente_id",
        "paciente_endereco_cep",
        "vacina_categoria_nome",
        "sistema_origem",
        "vacina_grupoAtendimento_nome",
        "paciente_nacionalidade_enumNacionalidade",
    ]
)

df = df.withColumn("ano", year(df["vacina_dataAplicacao"]))
df = df.withColumn("mes", month(df["vacina_dataAplicacao"]))


fabricante_map = {
    r"Pendente Identificação": "Pendente Identificação",
    r"Pendente Identifica\?\?o": "Pendente Identificação",
    r"ASTRAZENECA/FIOCRUZ": "Astrazeneca/Fiocruz",
    r"SINOVAC/BUTANTAN": "Sinovac/Butantan",
    r"PFIZER - PEDIÁTRICA MENOR DE 5 ANOS": "Pfizer - Pediátrica Menor De 5 Anos",
    r"PFIZER - PEDI\?TRICA": "Pfizer - Pediátrica",
    r"PFIZER - PEDIÁTRICA": "Pfizer - Pediátrica",
    r"(PFIZER)": "Pfizer",
    r"(JANSSEN)": "Janssen",
}

for original, substituto in fabricante_map.items():
    df = df.withColumn(
        "vacina_fabricante_nome",
        regexp_replace("vacina_fabricante_nome", original, substituto),
    )


df.write.parquet(
    path=SILVER_DATA_URI,
    partitionBy=["ano", "mes"],
    mode="overwrite",
)
