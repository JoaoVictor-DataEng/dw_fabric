# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ee12d855-0d70-4636-bb7c-4c877414a194",
# META       "default_lakehouse_name": "lakehouse_silver",
# META       "default_lakehouse_workspace_id": "bba0219c-62af-4fa9-a4da-84977313ebc8",
# META       "known_lakehouses": [
# META         {
# META           "id": "ee12d855-0d70-4636-bb7c-4c877414a194"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# importando módulos necessários
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configurando o ambiente para considerar datas anteriores a 1500/01/01
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lakehouse_bronze.tb_colaboradores_helloo")
df_empresas = spark.sql("SELECT * FROM lakehouse_bronze.tb_empresa_bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df\
        .filter(col("FOLHA") == "HELLOO")\
        .filter(col("CARGO").isNotNull())\
        .withColumnRenamed("TIPO DE CONTRATO", "REGIME")\
        .withColumnRenamed("DATA DA ÚLTIMA MOVIMENTAÇÃO", "DT_INICIO_SITUACAO")\
        .withColumnRenamed("COD CCUSTO", "COD_CCUSTO")\
        .withColumnRenamed("CENTRO DE CUSTO", "DESCRICAO_CCUSTO")\
        .withColumnRenamed("GESTOR DIRETO", "GESTOR")\
        .withColumnRenamed("DATA DE DESLIGAMENTO", "DEMISSAO")\
        .withColumnRenamed("ENDEREÇO COMPLETO", "ENDEREÇO_COMPLETO")\
        .withColumnRenamed("GRAU DE INSTRUÇÃO", "GRAU_DE_INSTRUÇÃO")\
        .withColumn("SHOPPING", when(col("SHOPPING") != "HELLOO", "HELLOO").otherwise(col("SHOPPING")))\
        .withColumn("CPF", lpad(col("CPF").cast("string"), 11, "0"))\
        .withColumn("É_HELLOO", lit("S"))\
        .withColumn("DIRETORIA", lit("DIRETORIA DE OPERAÇÕES"))\
        .withColumn("UNIDADE", lit("Shopping"))

df = df.select("EMPRESA", "CPF", "NOME", "CARGO", "REGIME", "SHOPPING", "UF", "SALARIO", "DT_INICIO_SITUACAO",
"ADMISSAO", "NASCIMENTO", "COD_CCUSTO", "DESCRICAO_CCUSTO", "GESTOR", "SEXO", "RAÇA", "GRAU_DE_INSTRUÇÃO",
"ENDEREÇO_COMPLETO", "PORTFÓLIO", "EMAIL", "DEMISSAO", "É_HELLOO", "DIRETORIA", "UNIDADE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Etapa criada para remover as duplicadas da coluna CPF mantendo as linhas NULL
df_distintos_nome = df.filter(col("CPF").isNull()).dropDuplicates(["NOME"])
df_distintos_cpf = df.filter(col("CPF").isNotNull()).dropDuplicates(["CPF"])
df = df_distintos_nome.unionByName(df_distintos_cpf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2 = df.select("NOME", col("CPF").alias("CPF_GESTOR"))
df = df\
    .join(df2, df["GESTOR"] == df2["NOME"], "left_outer")\
    .select(df["*"], df2["CPF_GESTOR"])

df = df\
    .join(df_empresas, df["EMPRESA"] == df_empresas["Razão Social"], "left_outer")\
    .select(df["*"], df_empresas["CNPJ"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tipos = {
    "SALARIO":"double",
    "DT_INICIO_SITUACAO":"timestamp",
    "ADMISSAO":"timestamp",
    "NASCIMENTO":"timestamp",
    "DEMISSAO":"timestamp"
}

for coluna, tipo in tipos.items():
    if tipo == "timestamp":
        df = df.withColumn(coluna, to_timestamp(col(coluna), "M/d/yyyy"))
    else:
        df = df.withColumn(coluna, col(coluna).cast(tipo))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Salvando a tabela

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("tb_colaboradores_helloo")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
