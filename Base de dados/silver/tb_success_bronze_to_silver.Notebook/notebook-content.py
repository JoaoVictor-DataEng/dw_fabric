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

# Importando módulos necessários
from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lakehouse_bronze.tb_success_bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Ajustando colunas

df = df\
    .withColumnRenamed("Cargo/Função Cargo", "Cargo/Função_Cargo")\
    .withColumnRenamed("Diretoria Executiva _N1_ Nome", "Diretoria_Nome")\
    .withColumnRenamed("Divisão _N2_", "DIRETORIA_N2")\
    .withColumn("DIRETORIA_N2", split(col("DIRETORIA_N2"), "-").getItem(1))\
    .filter( (col("CPF") != "") & (col("CPF") != "Admin_Luan"))\
    .withColumn('CPF', regexp_replace(col("CPF"), "[.-]", ""))\
    .withColumn("NOME", concat_ws(" ", col("Nome"), col("Sobrenome completo")))\
    .withColumn("NOME", upper(col("NOME")))\
    .withColumn(
        "Gestor_ajustado",
        concat_ws(" ",
            trim(split(col("Gestor"), ",").getItem(1)),
            trim(split(col("Gestor"), ",").getItem(0))))\
    .drop("Gestor")\
    .withColumnRenamed("Gestor_ajustado", "GESTOR")\
    .withColumn("GESTOR", upper(col("GESTOR")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fazendo self join para gerar o CPF do gestor

df_temp1 = df.alias("df_temp1")
df_temp2 = df.alias("df_temp2")
df = df_temp1\
    .join(df_temp2, col("df_temp1.GESTOR") == col("df_temp2.NOME"), "left_outer")\
    .select(*[col(f"df_temp1.{coluna}") for coluna in df_temp1.columns],
            col("df_temp2.CPF").alias("CPF_GESTOR"))

# Eliminando duplicadas

df = df\
    .withColumn("Detalhes do emprego Data da admissão", to_timestamp(col("Detalhes do emprego Data da admissão"), "dd/MM/yyyy"))\
    .sort(col("Detalhes do emprego Data da admissão").desc())\
    .dropDuplicates(["CPF"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Definindo colunas necessárias

df = df.select("Cargo/Função_Cargo", "Diretoria_Nome", "Usuário", "CNPJ", "Holding/Shopping", "CPF", "NOME", "GESTOR", "CPF_GESTOR", "DIRETORIA_N2")
ceo = df.filter(col("Cargo/Função_Cargo") == "DIRETOR(A) EXECUTIVO(A) PRESIDENTE").select("NOME").first()[0]
df = df.withColumn("CEO", lit(ceo))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Criando as tabelas de hierarquia

df = df.withColumn("ID_COLABORADOR", monotonically_increasing_id()) # Coluna de ID para gerar os joins

df1 = df.alias("df1")
df2 = df.alias("df2")

df = df1\
    .join(df2, col("df1.GESTOR") == col("df2.NOME"), "left_outer")\
    .select(
        *[col(f"df1.{c}") for c in df.columns],
        col("df2.ID_COLABORADOR").alias("ID_GESTOR_1"))

num_niveis = 8  # Número de níveis de gestores a criar
df_hierarquia = df.alias("df1")
df_hierarquia = df_hierarquia.withColumn("GESTOR_1", col("GESTOR"))

# Loop para criar os demais níveis de gestores
for i in range(2, num_niveis + 1):
    df_hierarquia = df_hierarquia.alias("df_hierarquia").join(
        df.alias(f"df{i}"),
        col(f"df_hierarquia.ID_GESTOR_{i-1}") == col(f"df{i}.ID_COLABORADOR"),"left_outer")\
        .select(
            *[col(f"df_hierarquia.{column_name}") for column_name in df_hierarquia.columns],  # Mantém tudo já criado
            when(col(f"df{i}.GESTOR") == ceo, None).otherwise(col(f"df{i}.GESTOR")).alias(f"GESTOR_{i}"),  # Remove CEO
            col(f"df{i}.ID_GESTOR_1").alias(f"ID_GESTOR_{i}")
            )

# Remover as colunas de ID_GESTOR, mantendo apenas os nomes dos gestores
colunas_finais = [column_name for column_name in df_hierarquia.columns if not column_name.startswith("ID_GESTOR_")]
df = df_hierarquia.select(*[col(column_name) for column_name in colunas_finais])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Criando a coluna 'PATH'

df = df\
    .withColumn("PATH",
    when(col("GESTOR_1") == lit(ceo), concat_ws(" | ", col("CEO"), col("NOME")))\
    .otherwise(concat_ws(" | ", col("CEO"), *[col(f"GESTOR_{i}") for i in range(num_niveis, 0, -1)], col("NOME"))))

colunas_finais = [column_name for column_name in df.columns if not column_name.startswith("GESTOR_")]
df = df.select(*colunas_finais).drop("CEO").drop("ID_COLABORADOR").dropDuplicates(["CPF"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Salvando a tabela
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("tb_success_silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
