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
from pyspark.sql.window import Window


# Configurando o ambiente para considerar datas anteriores a 1500/01/01
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Transformações básicas

# CELL ********************

# Importando as tabelas necessárias
df = spark.sql("SELECT * FROM lakehouse_bronze.tb_colaboradores_bronze")
df_empresas = spark.sql("SELECT * FROM lakehouse_bronze.tb_empresa_bronze")
df_success = spark.sql("SELECT * FROM tb_success_silver")
df_diretorias = spark.sql("SELECT * FROM lakehouse_bronze.tb_diretorias")
df_helloo = spark.sql("SELECT * FROM tb_colaboradores_helloo")
df_shoppings_administrados = spark.sql("SELECT * FROM lakehouse_bronze.tb_shoppings_administrados")
df_de_para_shoppings = spark.sql("SELECT * FROM lakehouse_bronze.tb_de_para_areas_shopping")
df_grade = spark.sql("SELECT * FROM lakehouse_bronze.tb_grade_bronze")
df_imagens_perfil = spark.sql("SELECT * FROM lakehouse_bronze.tb_imagens_perfil_bronze")
df_autodeclaracao_racial = spark.sql("SELECT * FROM lakehouse_bronze.tb_autodeclaracao_racial")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Renomeando colunas
df = df\
.withColumnRenamed("DATA RESCISAO", "DEMISSAO")\
.withColumnRenamed("EMPRESA COD_", "COD_EMPRESA")\
.withColumnRenamed("VINCULO", "REGIME")\
.withColumnRenamed("GESTOR NOME", "GESTOR_PROPAY")\
.withColumnRenamed("CAUSA RESCISÃO", "CAUSA_RESCISÃO")\
.withColumnRenamed("GRAU DE INSTRUÇÃO", "GRAU_DE_INSTRUCAO")

# Endereço completo
df = df\
    .withColumn("ENDEREÇO_COMPLETO",
                concat(col("ENDEREÇO"), lit(", "), col("NUMERO"), lit(", "), col("COMPLEMENTO"), lit(" - "), col("MUNICIPIO"), lit(" - "), col("UF")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Formatando as colunas

df = df\
    .withColumn("CPF", regexp_replace(col("CPF"), "[.-]", ""))\
    .withColumn("NOME", upper(col("NOME")))\
    .withColumn("CAUSA_RESCISÃO", upper(col("CAUSA_RESCISÃO")))\
    .withColumn("NACIONALIDADE", when(col("NACIONALIDADE").isNull(), "Brasileira").otherwise(col("NACIONALIDADE")))\
    .withColumn("EMAIL", lower(col("EMAIL")))\
    .withColumn("SEXO", upper(col("SEXO")))\
    .withColumn("CARGO", upper(col("CARGO")))\
    .withColumn("CHAVE", concat(lit("1:"), col("COD_EMPRESA"), lit(":"), col("MATRICULA") ))\
    .withColumn("SALARIO", regexp_replace(regexp_replace(col("SALARIO"), "\.", ""), ",", "."))\
    .withColumn("CIDADE_UF", concat_ws("-", col("MUNICIPIO"), col("UF")))\
    .withColumn("CEP", lpad(col("CEP").cast("string"), 8, "0"))\
    .withColumn("CEP", regexp_replace(col("CEP"), r"^(\d{5})(\d{3})$", r"$1-$2"))\
    .withColumn("NACIONALIDADE", upper(col("NACIONALIDADE")))\
    .withColumn("GRAU_DE_INSTRUCAO", upper("GRAU_DE_INSTRUCAO"))

window_spec = Window.partitionBy("CPF").orderBy("DT_INICIO_SITUACAO")
df = df\
    .withColumn("EMPRESA_ANTERIOR", lag("EMPRESA").over(window_spec))\
    .withColumn("CARGO_ANTERIOR", lag("CARGO").over(window_spec))\
    .withColumn("SALARIO_ANTERIOR", lag("SALARIO").over(window_spec))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mudando a tipagem de dados
tipos = {   
    "NASCIMENTO":"timestamp",
    "ADMISSAO":"timestamp",
    "SALARIO":"double",
    "SALARIO_ANTERIOR":"double",
    "DEMISSAO":"timestamp",
    "DT_INICIO_SITUACAO":"timestamp",
    }

for coluna, tipo in tipos.items():
    if tipo == "timestamp":
        df = df.withColumn(coluna, to_timestamp(col(coluna), "dd/MM/yyyy"))
    else:
        df = df.withColumn(coluna, col(coluna).cast(tipo))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mantendo somente as colunas necessárias
colunas = ['CHAVE', 'COD_EMPRESA', 'EMPRESA', 'CNPJ', 'MATRICULA', 'NOME', 'NASCIMENTO', 'SEXO', 'ADMISSAO', 'DEMISSAO', 'CARGO', 'SALARIO', 'REGIME',
            'ENDEREÇO_COMPLETO', 'NACIONALIDADE', 'RAÇA', 'COD_CCUSTO', 'DESCRICAO_CCUSTO', 'CAUSA_RESCISÃO', 'DT_INICIO_SITUACAO',
            'SITUAÇÃO', 'GRADE', 'EMAIL', 'CPF', 'PCD', 'GESTOR_PROPAY', 'UF', 'MUNICIPIO', 'CIDADE_UF', 'CEP',
            'NACIONALIDADE', 'GRAU_DE_INSTRUCAO', 'RAÇA', 'EMPRESA_ANTERIOR', 'CARGO_ANTERIOR', 'SALARIO_ANTERIOR']
df = df.select(colunas)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filtrando colunas
df = df\
    .filter((col("DEMISSAO").isNull()) | (col("DEMISSAO") > "2022-12-31"))\
    .filter(col("CAUSA_RESCISÃO") != "5 - TRANSFERÊNCIA SEM ÔNUS")\
    .filter(
        (~col("NOME").contains("RENATO FEITOSA RIQUE")) &
        (~col("NOME").contains("BERNADETE GONCALVES PEREIRA")) &
        (~col("NOME").contains("CARLOS EDUARDO CARNEIRO DOS SANTOS")) &
        (~col("CARGO").contains("CONSELH")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Lista de UFs e Estados
dados_estados = [
    ("AC", "Acre", "Norte"),
    ("AL", "Alagoas", "Nordeste"),
    ("AP", "Amapa", "Norte"),
    ("AM", "Amazonas", "Norte"),
    ("BA", "Bahia", "Nordeste"),
    ("CE", "Ceara", "Nordeste"),
    ("DF", "Distrito Federal", "Centro-Oeste"),
    ("ES", "Espirito Santo", "Sudeste"),
    ("GO", "Goias", "Centro-Oeste"),
    ("MA", "Maranhao", "Nordeste"),
    ("MT", "Mato Grosso", "Centro-Oeste"),
    ("MS", "Mato Grosso do Sul", "Centro-Oeste"),
    ("MG", "Minas Gerais", "Sudeste"),
    ("PA", "Para", "Norte"),
    ("PB", "Paraiba", "Nordeste"),
    ("PR", "Parana", "Sul"),
    ("PE", "Pernambuco", "Nordeste"),
    ("PI", "Piaui", "Nordeste"),
    ("RJ", "Rio de Janeiro", "Sudeste"),
    ("RN", "Rio Grande do Norte", "Nordeste"),
    ("RS", "Rio Grande do Sul", "Sul"),
    ("RO", "Rondonia", "Norte"),
    ("RR", "Roraima", "Norte"),
    ("SC", "Santa Catarina", "Sul"),
    ("SP", "Sao Paulo", "Sudeste"),
    ("SE", "Sergipe", "Nordeste"),
    ("TO", "Tocantins", "Norte")
]

# Criar DataFrame
df_estados = spark.createDataFrame(dados_estados, ["UF", "UF_NOME", "UF_REGIAO"])

df = df.join(df_estados, on="UF", how="left").select(df["*"], df_estados["UF_NOME"], df_estados["UF_REGIAO"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Criando o legado ajustado para considerar o primeiro legado do colaborador, ignorando mudança de legado em movimentações internas. Feito para turnover

# Janela por CPF ordenada por ADMISSAO
window_spec = Window.partitionBy("CPF").orderBy("ADMISSAO")

# Coluna com data da demissão anterior
df = df.withColumn("DEMISSAO_ANTERIOR", lag("DEMISSAO").over(window_spec))

# Coluna com PRIMEIRA_ADMISSAO válida
df = df.withColumn("CNPJ_AJUSTADO", when(
    (col("ADMISSAO").isNotNull()) & (col("DEMISSAO_ANTERIOR").isNotNull()) &
    (
        (col("ADMISSAO") >= lit("2023-01-01")) |
        ((col("ADMISSAO") >= lit("2023-01-01")) & (datediff(col("ADMISSAO"), col("DEMISSAO_ANTERIOR")) > 90))
    ),
    lit("05.878.397/0001-32") # CNPJ ALLOS
).when(
    (col("ADMISSAO").isNotNull()) & (col("DEMISSAO_ANTERIOR").isNotNull()) &
    (datediff(col("ADMISSAO"), col("DEMISSAO_ANTERIOR")) <= 90),
    col("CNPJ")
).otherwise(col("CNPJ")))

# Criando índice por CPF para pegar a primeira linha
window_first = Window.partitionBy("CPF").orderBy("ADMISSAO")
df = df.withColumn("row_num", row_number().over(window_first))

# Pegando o CNPJ_AJUSTADO da primeira admissão válida por CPF
primeiro_cnpj_df = df.filter(col("row_num") == 1).select("CPF", col("CNPJ_AJUSTADO").alias("CNPJ_AJUSTADO_FINAL"))

# Join de volta no DataFrame original
df_final = df.join(primeiro_cnpj_df, on="CPF", how="left")
df = df_final\
    .drop("row_num")\
    .drop("DEMISSAO_ANTERIOR")\
    .drop("CNPJ_AJUSTADO")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Primeiro join
df_empresas_a = df_empresas.alias("a")
df = df.alias("df") \
    .join(df_empresas_a, on="CNPJ", how="left") \
    .select(
        col("df.*"),
        col("a.Unidade").alias("UNIDADE"),
        col("a.Shopping").alias("SHOPPING"),
        col("a.Empresa").alias("LEGADO"),
        col("a.Portfólio").alias("PORTFÓLIO"),
        col("a.Estado").alias("ESTADO_SHOPPING"),\
        col("a.Região").alias("REGIAO_SHOPPING"))\
    .filter((col("UNIDADE") != "") & (col("UNIDADE").isNotNull()))
    #.withColumn("ESTADO_SHOPPING", when(col("ESTADO_SHOPPING").isNull(), col("UF_NOME")).otherwise(col("ESTADO_SHOPPING")))

# Segundo join, com outro alias para df_empresas
df_empresas_b = df_empresas.alias("b")
df = df.alias("df2") \
    .join(df_empresas_b, col("df2.CNPJ_AJUSTADO_FINAL") == col("b.CNPJ"), "left_outer") \
    .select(
        col("df2.*"),
        col("b.Empresa").alias("LEGADO_Ajustado")
    )

# Removendo duplicadas
df = df\
    .sort(col("CPF").asc(), col("DEMISSAO").asc(), col("SALARIO").desc(), col("DT_INICIO_SITUACAO").desc())\
    .dropDuplicates(["CPF"])\
    .drop("CNPJ_AJUSTADO_FINAL")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Join com a tabela success

# CELL ********************

# Fazendo join com a tabela success
df = df\
    .join(df_success, df["CPF"] == df_success["CPF"], "left_outer")\
    .select(df["*"], df_success["Usuário"], df_success["GESTOR"].alias("GESTOR_SUCCESS"), df_success["CPF_GESTOR"].alias("CPF_GESTOR_SUCCESS"),
     df_success["PATH"], df_success["Diretoria_Nome"], df_success["DIRETORIA_N2"])

df1 = df.alias("df1")
df2 = df.alias("df2")

# Self join para trazer o CPF do gestor
df = df1\
    .join(df2, col("df1.GESTOR_PROPAY") == col("df2.NOME"), "left_outer")\
    .select(*[col(f"df1.{c}") for c in df.columns], col("df2.CPF").alias("CPF_GESTOR_PROPAY"))

lista_cpf = [row["CPF"] for row in df.select("CPF").distinct().collect() if row["CPF"] not in (None, "")]

# O ajuste de CPF e gestor abaixo é feito para que seja apenas considerado do success os gestores que
# existirem previamente na propay como colaboradores
df = df\
    .withColumn("EMAIL", when( (col("EMAIL") == "") | (col("EMAIL").isNull()), col("Usuário")).otherwise(col("EMAIL")))\
    .withColumn("GESTOR",
        when((col("CPF_GESTOR_SUCCESS").isin(lista_cpf)) & (col("CPF_GESTOR_SUCCESS").isNotNull()), col("GESTOR_SUCCESS")).otherwise(col("GESTOR_PROPAY")))\
    .withColumn("CPF_GESTOR",
        when((col("CPF_GESTOR_SUCCESS").isin(lista_cpf))  & (col("CPF_GESTOR_SUCCESS").isNotNull()), col("CPF_GESTOR_SUCCESS")).otherwise(col("CPF_GESTOR_PROPAY")))\
    .drop("Usuário")\
    .drop("CPF_GESTOR_PROPAY")\
    .drop("GESTOR_PROPAY")\
    .drop("CPF_GESTOR_SUCCESS")\
    .drop("GESTOR_SUCCESS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Populando as colunas GESTOR e CPF_GESTOR

# CELL ********************

# Criando as tabelas auxiliares para ajustar os campos de gestor e CPF dos colaboradores de shopping
# Aqui foi usado .alias() para que não houvesse ambiguidade ao realizar os joins posteriormente
df_superintendentes = df.alias("df_superintendentes")\
    .filter(col("CARGO").contains("SUPERINTENDENTE"))\
    .filter(col("DEMISSAO").isNull())\
    .sort(col("DT_INICIO_SITUACAO").desc())\
    .dropDuplicates(["CNPJ"])\
    .select("CNPJ", "EMPRESA", "NOME", "CPF")

# O .alias() aqui é colocado no final porque como há uma alteração no nome das colunas, a referência do .alias() seria perdida
df_diretores_portfolio = df_empresas\
    .filter(col("Unidade") == "Shopping")\
    .dropDuplicates(["CNPJ"])\
    .withColumnRenamed("Nome diretor", "Nome_diretor")\
    .withColumnRenamed("CPF diretor", "CPF_diretor")\
    .select("CNPJ", "Shopping", "Nome_diretor", "CPF_diretor")\
    .alias("df_diretores_portfolio")

df1 = df.alias("df1")

# Realizando os joins
df = df1\
.join(df_superintendentes, col("df1.CNPJ") == col("df_superintendentes.CNPJ"), "left_outer")\
.join(df_diretores_portfolio, col("df1.CNPJ") == col("df_diretores_portfolio.CNPJ"), "left_outer")\
.select(*[col(f"df1.{c}") for c in df1.columns],
        col("df_superintendentes.NOME").alias("Nome_superintendente"),
        col("df_superintendentes.CPF").alias("CPF_superintendente"),
        col("df_diretores_portfolio.Nome_diretor").alias("Nome_regional"),
        col("df_diretores_portfolio.CPF_diretor").alias("CPF_regional"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Ajustando a coluna GESTOR
df = df\
    .withColumn("GESTOR",
        when((col("Unidade") == "Shopping") & (col("CPF_GESTOR").isNull()) & (col("CPF_superintendente").isNull()), col("Nome_regional"))\
        .otherwise(
            when((col("Unidade") == "Shopping") & (col("CPF_GESTOR").isNull()) & (col("CPF_superintendente").isNotNull()), col("Nome_superintendente"))\
            .otherwise(col("GESTOR"))))

# Ajustando a coluna CPF
df = df\
    .withColumn("CPF_GESTOR",
        when((col("Unidade") == "Shopping") & (col("CPF_GESTOR").isNull()) & (col("CPF_superintendente").isNull()), col("CPF_regional"))\
        .otherwise(
            when((col("Unidade") == "Shopping") & (col("CPF_GESTOR").isNull()) & (col("CPF_superintendente").isNotNull()), col("CPF_superintendente"))\
            .otherwise(col("CPF_GESTOR"))))

df = df\
    .drop("Nome_superintendente")\
    .drop("CPF_superintendente")\
    .drop("Nome_regional")\
    .drop("CPF_regional")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Armazenando as tabelas demissões e onboarding
df_demissoes = df\
    .withColumn("FILTRO_TRANSFERÊNCIA", lit(1))\
    .filter(col("DEMISSAO").isNull())\
    .select("DEMISSAO", "CPF", "FILTRO_TRANSFERÊNCIA")\
    .dropDuplicates(["CPF"])

df_onboarding = df\
    .groupBy(col("CPF"))\
    .agg(count("CPF").alias("ONBOARDING_REGRA"))\
    .filter(col("ONBOARDING_REGRA") > 1)\
    .select("CPF", "ONBOARDING_REGRA")\
    .dropDuplicates(["CPF"])  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fazendo join com as tabelas demissões e onboarding
df = df\
    .join(df_demissoes, df["CPF"] == df_demissoes["CPF"], "left_outer")\
    .select(df["*"], df_demissoes["FILTRO_TRANSFERÊNCIA"])

df = df\
    .join(df_onboarding, df["CPF"] == df_onboarding["CPF"], "left_outer")\
    .select(df["*"], df_onboarding["ONBOARDING_REGRA"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Colunas de grade e imagem

# CELL ********************

# Trazendo a coluna '100%' da tabela tb_grade_bronze e coluna 'Link imagem' da tabela tb_imagens_perfil_bronze
df = df\
    .join(df_grade, df["GRADE"] == df_grade["Grade Salarial"], "left_outer")\
    .join(df_imagens_perfil, df_imagens_perfil["CPF"] == df["CPF"], "left_outer")\
    .select(df["*"], df_grade["100%"], df_imagens_perfil["Link imagem"])

df = df\
    .withColumnRenamed("100%", "SALARIO_100%")\
    .withColumnRenamed("Link imagem", "Link_imagem")\
    .withColumn("SALARIO_100%", col("SALARIO_100%").cast("double"))

# Inserindo colunas de faixa de grade
df = df\
    .withColumn("FAIXA_GRADE", col("SALARIO") / col("SALARIO_100%"))\
    .withColumn("FAIXA_GRADE", round(col("FAIXA_GRADE"), 2))\
    .withColumn("FAIXA_GRADE_INDEX", 
                                when(col("FAIXA_GRADE") == 0, 0)
                               .when(col("FAIXA_GRADE") < 0.8, 1)
                               .when((col("FAIXA_GRADE") >= 0.8) & (col("FAIXA_GRADE") < 0.9), 2)
                               .when((col("FAIXA_GRADE") >= 0.9) & (col("FAIXA_GRADE") < 1), 3)
                               .when((col("FAIXA_GRADE") >= 1) & (col("FAIXA_GRADE") < 1.1), 4)
                               .when((col("FAIXA_GRADE") >= 1.1) & (col("FAIXA_GRADE") < 1.2), 5)
                               .when(col("FAIXA_GRADE") == 1.2, 6)
                               .otherwise(7))\
    .orderBy("FAIXA_GRADE_INDEX")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Criação da hierarquia

# CELL ********************

# Nome do CEO
ceo = df.filter(col("CARGO") == "DIRETOR(A) EXECUTIVO(A) PRESIDENTE") \
         .select("NOME").first()[0]

# Adiciona coluna CEO
df = df.withColumn("CEO", lit(ceo))

# Cria um ID único
window = Window.orderBy("NOME")
df = df.withColumn("ID_COLABORADOR", row_number().over(window))

# Relação colaborador -> gestor imediato
df_relacao = df.select(
    col("ID_COLABORADOR"),
    col("NOME").alias("NOME_COLAB"),
    col("GESTOR").alias("GESTOR_IMEDIATO")
)

# Mapa Nome -> Gestor
df_mapa = df.select(
    col("NOME").alias("NOME_GESTOR"),
    col("GESTOR").alias("GESTOR_ACIMA")
)

# Número de níveis da hierarquia
num_niveis = 5

# Começa com colaborador e gestor imediato
df_hierarquia = df_relacao.withColumn(
    "HIERARQUIA", array(col("GESTOR_IMEDIATO"))
)

# Itera subindo na hierarquia
for i in range(2, num_niveis + 1):
    # Faz join para pegar o gestor acima do último gestor adicionado
    df_hierarquia = df_hierarquia.join(
        df_mapa,
        df_hierarquia[f"HIERARQUIA"].getItem(i - 2) == df_mapa.NOME_GESTOR,
        "left"
    ).withColumn(
        "HIERARQUIA",
        concat(
            col("HIERARQUIA"),
            array(col("GESTOR_ACIMA"))
        )
    ).drop("NOME_GESTOR", "GESTOR_ACIMA")

# Explode o array em colunas GESTOR_1 ... GESTOR_n
for i in range(num_niveis):
    df_hierarquia = df_hierarquia.withColumn(
        f"GESTOR_{i+1}", col("HIERARQUIA").getItem(i)
    )

# Remove array auxiliar
df_hierarquia = df_hierarquia\
                .drop("HIERARQUIA")\
                .drop("GESTOR_IMEDIATO")\
                .drop("NOME_COLAB")

# Junta com o DataFrame original
df = df.join(df_hierarquia, on="ID_COLABORADOR", how="left").drop("ID_COLABORADOR")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Criando a coluna 'PATH_HIERARQUIA' e 'NIVEIS_HIERARQUIA'
colunas = [
    when((col(f"GESTOR_{i}") == "") | (col(f"GESTOR_{i}") == ceo), None)
    .otherwise(col(f"GESTOR_{i}"))
    for i in range(num_niveis, 0, -1)
]

df = (
    df.withColumn(
        "PATH_HIERARQUIA",
        when(
            col("GESTOR_1") == lit(ceo),
            concat_ws(" | ", col("CEO"), col("NOME"))
        ).otherwise(
            concat_ws(" | ", col("CEO"), *colunas, col("NOME"))
        )
    )
)

# Remove colunas temporárias de gestores
colunas_finais = [
    column_name for column_name in df.columns
    if not column_name.startswith("GESTOR_")
]
df = df.select(*colunas_finais)

# Calcula quantidade de níveis da hierarquia
df = df.withColumn(
    "NIVEIS_HIERARQUIA",
    size(split(col("PATH_HIERARQUIA"), "\|"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Definindo o número máximo de níveis
max_niveis = df.agg(max("NIVEIS_HIERARQUIA")).collect()[0][0]  # Obtém o maior nível de hierarquia

df = df.withColumn("PATH_ARRAY", split(col("PATH_HIERARQUIA"), "\|"))

# Criando dinamicamente colunas NIVEL_1, NIVEL_2, ..., NIVEL_N
for i in range(1, max_niveis + 1):
    df = df.withColumn(f"NIVEL_{i}", col("PATH_ARRAY").getItem(i - 1))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### UDF para ajustar a coluna DIRETORIA

# CELL ********************

# 1. Transforma df_diretorias em lista de tuplas (NOME, DIRETORIA)
lista_diretorias = df_diretorias.select("NOME", "DIRETORIA") \
    .rdd.map(lambda row: (row["NOME"], row["DIRETORIA"])) \
    .collect()

# 2. Cria UDF para verificar se o nome está contido no PATH
def encontrar_diretoria(path):
    if path is None:
        return None
    for nome, diretoria in lista_diretorias:
        if nome in path:
            return diretoria
    return None

# 3. Registra a UDF
udf_encontrar_diretoria = udf(encontrar_diretoria, StringType())

# 4. Aplica a UDF ao df para gerar a nova coluna
df = df\
    .withColumn("DIRETORIA", udf_encontrar_diretoria(col("PATH_HIERARQUIA")))\
    .withColumn("DIRETORIA", when(
                                    (col("UNIDADE") == 'Shopping') & (col("DIRETORIA").isNull()), lit("DIRETORIA DE OPERAÇÕES")).otherwise(col("DIRETORIA")))\
    .withColumn("DIRETORIA", when(col("DIRETORIA").isNull(), col("Diretoria_Nome")).otherwise(col("DIRETORIA")))\
    .drop("Diretoria_Nome")\
    .drop("PATH_ARRAY")\
    .drop("PATH")\
    .drop("CEO")\
    #.drop("PATH_HIERARQUIA")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Tabelas helloo e shoppings administrados

# CELL ********************

# Lista com os três DataFrames
dfs = [df, df_helloo, df_shoppings_administrados]

# Passo 1: descobrir todas as colunas únicas
todas_colunas = set()
for d in dfs:
    todas_colunas.update(d.columns)

todas_colunas = list(todas_colunas)

# Passo 2: garantir que todos os DataFrames tenham todas as colunas, mesmo que como null
def padronizar_colunas(df, todas_colunas):
    for col in todas_colunas:
        if col not in df.columns:
            df = df.withColumn(col, lit(None))
    return df.select(todas_colunas)

# Padronizando os DataFrames
dfs_padronizados = [padronizar_colunas(d, todas_colunas) for d in dfs]

# Passo 3: unir todos corretamente
df_unificado = dfs_padronizados[0]
for d in dfs_padronizados[1:]:
    df_unificado = df_unificado.unionByName(d)
df = df_unificado

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### UDF para criar a coluna ÁREA_SHOPPING

# CELL ********************

# Coleta o de-para localmente
de_para = df_de_para_shoppings.select("CARGO SHOPPING", "ÁREA SHOPPING").collect()

# Converte para lista de tuplas
de_para_list = [(row["CARGO SHOPPING"], row["ÁREA SHOPPING"]) for row in de_para]

# Define a UDF
def buscar_area(unidade, cargo):
    if unidade != "Shopping" or cargo is None:
        return None
    for cargo_ref, area in de_para_list:
        if cargo_ref in cargo:
            return area
    return None

buscar_area_udf = udf(buscar_area, StringType())

# Aplica a UDF
df = df.withColumn("ÁREA_SHOPPING", buscar_area_udf(col("UNIDADE"), col("CARGO")))

# Organizando as colunas
df = df.select(sorted(df.columns))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Incluindo dados de raça

# CELL ********************

df = df\
    .join(df_autodeclaracao_racial, df["CPF"] == df_autodeclaracao_racial["CPF"], "left_outer")\
    .select(df["*"],
            df_autodeclaracao_racial['Você_já_se_sentiu_desconfortável_em_algum_lugar_por_causa_da_sua_cor?'],
            df_autodeclaracao_racial['Você_já_ouviu_piadas_por_conta_da_cor_da_sua_pele_ou_do_seu_tipo_de_cabelo?'],
            df_autodeclaracao_racial['Você_já_desejou_ter_outra_cor_de_pele?'],
            df_autodeclaracao_racial['Com_qual_cor_ou_raça_você_se_identifica?'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Verificação da quantidade de linhas

# CELL ********************

# # Todos
# display(df.count())

# # Ativos sem helloo e sem shoppings administrados
# display(df.filter(col("DEMISSAO").isNull()).filter(col("É_HELLOO").isNull()).filter(col("É_SHOPPING_ADM").isNull()).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Salvando a tabela

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Salvando a tabela tb_colaboradores

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("tb_colaboradores_silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
