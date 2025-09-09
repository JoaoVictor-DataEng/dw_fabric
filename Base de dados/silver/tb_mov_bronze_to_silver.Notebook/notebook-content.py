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
# META       "default_lakehouse_workspace_id": "bba0219c-62af-4fa9-a4da-84977313ebc8"
# META     }
# META   }
# META }

# CELL ********************

# Importando os módulos principais

from pyspark.sql.functions import *
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lakehouse_bronze.tb_mov_bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Renomeando e ajustando colunas

df = df\
    .withColumnRenamed("DATA ALTERAÇÃO", "DataAlteracao")\
    .withColumnRenamed("CARGO NA DATA DA OCORRÊNCIA", "Cargo")\
    .withColumnRenamed("EMP", "Codigo")\
    .withColumnRenamed("NOME", "Nome")\
    .withColumnRenamed("ADMISSÃO", "DataAdmissao")\
    .withColumnRenamed("MATRICULA", "Matricula")\
    .withColumnRenamed("SALARIAL ATUAL", "Salário")\
    .withColumnRenamed("VARIAÇÃO %", "Variacao")\
    .withColumnRenamed("NOME_MOTIVO", "Motivo")\
    .withColumn("CPF", regexp_replace(col("CPF"), "[.-]", ""))\
    .withColumn('Chave', concat(lit('1:'), col('Codigo'), lit(':'), col('Matricula') ))\
    .withColumn('Chave_Empresa', concat(lit('1:'), col('Codigo')))\
    .withColumn("Salário", regexp_replace(regexp_replace(col("Salário"), "\.", ""), ",", ".")) #ajustando coluna SALARIO para conseguir realizar a conversão do tipo

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn(
    "Variacao",
    regexp_replace("Variacao", "%", "")  # Remove o símbolo de porcentagem
)

df = df\
    .withColumn("Variacao", regexp_replace("Variacao", ",", "."))\
    .withColumn("Variacao", col("Variacao").cast("double")/100)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mudando tipagem de dados

tipos = {
        "Chave":"string",
        "Chave_Empresa":"string",
        "CPF":"string",
        "DataAlteracao":"timestamp",
        "Cargo":"string",
        "Nome":"string",
        "DataAdmissao":"timestamp",
        "Salário":"double",
        "Motivo":"string",
}

for coluna, tipo in tipos.items():
    if tipo == "timestamp":
        df = df.withColumn(coluna, to_timestamp(col(coluna), "dd/MM/yyyy"))
    else:
        df = df.withColumn(coluna, col(coluna).cast(tipo))

df = df\
    .select(
            "Chave",
            "Chave_Empresa",
            "CPF",
            "DataAlteracao",
            "Cargo",
            "Nome",
            "DataAdmissao",
            "Salário",
            "Motivo",
            "Variacao")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Salvando a tabela

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("tb_mov_silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
