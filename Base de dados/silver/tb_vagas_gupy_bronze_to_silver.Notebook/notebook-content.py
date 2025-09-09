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

# Importando os módulos principais

from pyspark.sql.functions import *
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lakehouse_bronze.tb_vagas_gupy_bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Selecionando colunas necessárias

colunas = [
    "Requisição",
    "Status da Vaga",
    "Filial",
    "Gerente da vaga",
    "Email Gerente",
    "Cargo",
    "Tipo de Publicação",
    "Página de Carreiras",
    "Recrutador da vaga",
    "Data de Criação",
    "Data de Publicação",
    "Data de Fechamento",
    "Última Data do Congelamento",
    "Última Data de Descongelamento",
    "Data de cancelamento",
    "Tipo de Vaga_1",
    "Tipo de vaga",
    "Unidade de Negócio",
    "PCD",
    "Diretoria Executiva",
    "Tipo de recrutamento",
    "Área/Regional",
    "Vaga PCD",
    "Motivo"]

df = df.select(colunas)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Ajustando colunas

df = df\
    .filter(col("Tipo de vaga") != "Banco de talentos")\
    .withColumnRenamed("Filial", "Shopping")\
    .withColumnRenamed("Tipo de vaga", "Objetivo_da_vaga")\
    .withColumnRenamed("Tipo de Vaga_1", "Tipo_de_Vaga")\
    .withColumnRenamed("Status da Vaga", "Status_da_Vaga")\
    .withColumnRenamed("Email Gerente", "Email_Gerente")\
    .withColumnRenamed("Tipo de Publicação", "Tipo_de_Publicação")\
    .withColumnRenamed("Unidade de Negócio", "Unidade_de_Negócio")\
    .withColumnRenamed("Recrutador da vaga", "Recrutador_da_vaga")\
    .withColumnRenamed("Página de Carreiras", "Página_de_Carreiras")\
    .withColumnRenamed("Gerente da vaga", "Gerente_da_vaga")\
    .withColumnRenamed("Diretoria Executiva", "Diretoria_Executiva")\
    .withColumnRenamed("Tipo de recrutamento", "Tipo_de_recrutamento")\
    .withColumnRenamed("Data de cancelamento", "Data_de_cancelamento")\
    .withColumnRenamed("Data de Criação", "Data_de_Criação")\
    .withColumnRenamed("Data de Publicação","Data_de_Publicação")\
    .withColumnRenamed("Data de Fechamento", "Data_de_Fechamento")\
    .withColumnRenamed("Última Data do Congelamento", "Última_Data_do_Congelamento")\
    .withColumnRenamed("Última Data de Descongelamento", "Última_Data_de_Descongelamento")\
    .withColumnRenamed("Data de cancelamento", "Data_de_cancelamento")\
    .withColumnRenamed("Vaga PCD", "Vaga_PCD")

df = df\
    .withColumn("Área/Regional", upper(col("Área/Regional")))\
    .withColumn("Unidade_de_Negócio", upper(col("Unidade_de_Negócio")))\
    .withColumn("Recrutador_da_vaga", upper(col("Recrutador_da_vaga")))\
    .withColumn("Cargo", upper(col("Cargo")))\
    .withColumn("Gerente_da_vaga", upper(col("Gerente_da_vaga")))\
    .withColumn("Diretoria_Executiva", upper(col("Diretoria_Executiva")))\
    .withColumn("Tipo_de_recrutamento", upper(col("Tipo_de_recrutamento")))\
    .withColumn("Aprendiz_PCD",
                when(col("Objetivo_da_vaga") == "Aprendiz", lit("Aprendiz"))\
                .when(col("PCD") == "Obrigatório", lit("PCD")))\
    .withColumn("Data_de_cancelamento", split(col("Data_de_cancelamento"), " ")[0]) # Removendo o texto após o espaço em branco


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

# Aplicando as transformações na coluna 'Área/Regional'
df = df.withColumn(
    "Área/Regional",
    when(col("Área/Regional").like("%DANIEL FREITAS%"), "DANIEL FREITAS")
    .when(col("Área/Regional").like("%HOLDING%"), "HOLDING")
    .when(
        (col("Área/Regional").like("%NORDESTE%")) |
        ((col("Área/Regional").like("%S%")) & (col("Área/Regional").like("%O PAULO%"))),
        "RICARDO VISCO")
    .otherwise(col("Área/Regional")))


df = df\
    .withColumn("Área/Regional", regexp_replace(col("Área/Regional"), "DIR. PORTFÓLIO - ", ""))\
    .withColumn("Área/Regional", regexp_replace(col("Área/Regional"), "DIRETOR POTFOLIO - ", ""))\
    .withColumn("Área/Regional", regexp_replace(col("Área/Regional"), " (0001)", ""))\
    .withColumn("Área/Regional", regexp_replace(col("Área/Regional"), " (0010)", ""))

# Aplicando as transformações na coluna 'Diretoria_Executiva'
df = df.withColumn(
    "Diretoria_Executiva",
    when(col("Diretoria_Executiva").like("%TECNOLOGIA%"), "DIRETORIA DE INOVAÇÃO E TECNOLOGIA")
    .when(
        (col("Diretoria_Executiva").like("%OPERA%") & col("Diretoria_Executiva").like("%ES%")) |
        col("Diretoria_Executiva").like("%RICARDO VISCO%") |
        col("Diretoria_Executiva").like("%SERGIO PESSOA%"),
        "DIRETORIA DE OPERAÇÕES"
    )
    .when(col("Diretoria_Executiva").like("%FINANCEIR%"), "DIRETORIA FINANCEIRA E RI")
    .when(
        col("Diretoria_Executiva").like("%JUR%") & col("Diretoria_Executiva").like("%DIC%"),
        "DIRETORIA JURÍDICA"
    )
    .otherwise(col("Diretoria_Executiva"))
)

# Aplicando as transformações na coluna 'Shopping'
df = df.withColumn("Shopping", regexp_replace(col("Shopping"), "BRMALLS > ", ""))

# Aplicando as transformações na coluna 'Tipo_de_recrutamento'
df = df.withColumn(
    "Tipo_de_recrutamento",
    when(col("Tipo_de_recrutamento").like("%EXTERNA%"), "EXTERNO")
    .otherwise(col("Tipo_de_recrutamento"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mudando a tipagem dos dados

tipos = {
    "Data_de_Criação":"timestamp",
    "Data_de_Publicação":"timestamp",
    "Data_de_Fechamento":"timestamp",
    "Última_Data_do_Congelamento":"timestamp",
    "Última_Data_de_Descongelamento":"timestamp",
    "Data_de_cancelamento":"timestamp",
}

for coluna, tipo in tipos.items():
    if tipo == "timestamp":
        df = df.withColumn(coluna, to_timestamp(col(coluna), "yyyy-MM-dd"))
    else:
        df = df.withColumn(coluna, col(coluna).cast(tipo))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Incluindo a coluna 'Dias abertos'

df = df\
    .withColumn("Dias_abertos",
         when(col("Data_de_cancelamento").isNotNull(), datediff(col("Data_de_cancelamento"), col("Data_de_Publicação")))\
        .when(col("Data_de_Fechamento").isNotNull(), datediff(col("Data_de_Fechamento"), col("Data_de_Publicação")))\
        .otherwise(datediff(current_timestamp(), col("Data_de_Publicação")))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Incluindo a coluna 'Unidade_negócio'

df = df\
    .withColumn("Unidade_negócio",
        when( 
            ( col("Unidade_de_Negócio").contains("HOLDING") ) | ( col("Unidade_de_Negócio").contains("HELLOO") ),
            "Holding")\
        .when(
            ( col("Unidade_de_Negócio").isNull() ) | ( col("Unidade_de_Negócio") == "" ),
            "Shopping")\
        .otherwise("Shopping")
        )\
    .drop("Unidade_de_Negócio")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Incluindo a coluna 'Líder/Não_líder'

df = df\
    .withColumn("Líder/Não_líder",
        when( 
            ( col("Tipo_de_Vaga") == "Não Líder" ) | ( col("Tipo_de_Vaga") == "Campo" ),
            "Não Líder")\
        .when(
            ( col("Tipo_de_Vaga") == "Líder" ),
            "Líder")\
        .otherwise(col("Tipo_de_Vaga"))
        )\
    .drop("Tipo_de_Vaga")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("tb_vagas_gupy_silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
