from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from src.settings import SPARK_CONNECT_ENDPOINT, DatalakeZones

SILVER_DATA_URI = DatalakeZones.SILVER.to_s3_uri(
    spark_fmt=True,
    prefix="vacinacao_covid",
)

GOLD_PACIENTE_DIM_URI = DatalakeZones.GOLD.to_s3_uri(
    prefix="paciente_dim",
)

GOLD_ESTABELECIMENTO_DIM_URI = DatalakeZones.GOLD.to_s3_uri(
    prefix="estabelecimento_dim",
)

GOLD_VACINA_GRUPO_DIM_URI = DatalakeZones.GOLD.to_s3_uri(
    prefix="vacina_grupo_dim",
)

GOLD_VACINA_CATEGORIA_DIM_URI = DatalakeZones.GOLD.to_s3_uri(
    prefix="vacina_categoria_dim",
)

GOLD_VACINA_DIM_URI = DatalakeZones.GOLD.to_s3_uri(
    prefix="vacina_dim",
)

GOLD_DATA_DIM_URI = DatalakeZones.GOLD.to_s3_uri(
    prefix="data_dim",
)

GOLD_VACINA_FACT_URI = DatalakeZones.GOLD.to_s3_uri(
    prefix="vacina_fact",
)

spark: SparkSession = (
    SparkSession.builder.appName("Silver to Gold")  # type: ignore
    .remote(SPARK_CONNECT_ENDPOINT)
    .getOrCreate()
)

df: DataFrame = spark.read.format("parquet").load(SILVER_DATA_URI)

paciente_dim = df.select(
    "paciente_id",
    "paciente_idade",
    "paciente_dataNascimento",
    "paciente_enumSexoBiologico",
    "paciente_racaCor_codigo",
    "paciente_endereco_coIbgeMunicipio",
    "paciente_endereco_coPais",
    "paciente_endereco_uf",
    "paciente_nacionalidade_enumNacionalidade",
).dropDuplicates()

estabelecimento_dim = df.select(
    "estabelecimento_valor",
    "estabelecimento_razaoSocial",
    "estalecimento_noFantasia",
    "estabelecimento_municipio_codigo",
).dropDuplicates()

vacina_grupo_dim = df.select(
    "vacina_grupoAtendimento_codigo", "vacina_grupoAtendimento_nome"
).dropDuplicates()

vacina_categoria_dim = df.select(
    "vacina_categoria_codigo", "vacina_categoria_nome"
).dropDuplicates()

vacina_dim = df.select("vacina_fabricante_nome", "vacina_nome").dropDuplicates()

data_dim = df.select("vacina_dataAplicacao", "ano", "mes").dropDuplicates()

df = df.select(
    *[
        "paciente_id",
        "estabelecimento_valor",
        "vacina_grupoAtendimento_codigo",
        "vacina_categoria_codigo",
        "vacina_fabricante_nome",
        "vacina_dataAplicacao",
    ]
)

vacina_fact = (
    df.join(paciente_dim, "paciente_id")
    .join(estabelecimento_dim, "estabelecimento_valor")
    .join(vacina_grupo_dim, "vacina_grupoAtendimento_codigo")
    .join(vacina_categoria_dim, "vacina_categoria_codigo")
    .join(vacina_dim, "vacina_fabricante_nome")
    .join(data_dim, "vacina_dataAplicacao")
)

paciente_dim.write.parquet(
    path=GOLD_PACIENTE_DIM_URI,
    mode="overwrite",
)
estabelecimento_dim.write.parquet(
    path=GOLD_ESTABELECIMENTO_DIM_URI,
    mode="overwrite",
)
vacina_grupo_dim.write.parquet(
    path=GOLD_VACINA_GRUPO_DIM_URI,
    mode="overwrite",
)
vacina_categoria_dim.write.parquet(
    path=GOLD_VACINA_CATEGORIA_DIM_URI,
    mode="overwrite",
)
vacina_dim.write.parquet(
    path=GOLD_VACINA_DIM_URI,
    mode="overwrite",
)
data_dim.write.parquet(
    path=GOLD_DATA_DIM_URI,
    mode="overwrite",
)

vacina_fact.write.parquet(
    path=GOLD_VACINA_FACT_URI,
    mode="overwrite",
)
