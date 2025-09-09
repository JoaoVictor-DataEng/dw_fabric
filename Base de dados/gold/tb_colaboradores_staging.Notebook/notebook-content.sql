-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "a0d35ad1-d3ba-89aa-48e0-e523d6ea004f",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "a0d35ad1-d3ba-89aa-48e0-e523d6ea004f",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- Removendo a tabela se ela já existir

DROP TABLE IF EXISTS dbo.tb_colaboradores_staging;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

-- Criando a CTE

WITH CTE_temp AS (
    SELECT
        *,
        CASE
            WHEN CARGO LIKE '%ADV%' OR CARGO LIKE '%ADVOGADO%' THEN 'ADVOGADO'
            WHEN CARGO LIKE '%ANALISTA%' OR CARGO LIKE 'ANL%' THEN 'ANALISTA'
            WHEN CARGO LIKE '%APRENDIZ%' THEN 'APRENDIZ'
            WHEN CARGO LIKE '%ASSIST%' THEN 'ASSISTENTE'
            WHEN CARGO LIKE '%DIRETOR%EXECUTIV%PRESIDENT%' THEN 'CEO'
            WHEN CARGO LIKE '%COORD%' OR CARGO LIKE '%COORDENADOR%' THEN 'COORDENADOR'
            WHEN CARGO LIKE '%DIRETOR%EXECUTIV%' OR
                CARGO LIKE 'PRESIDENTE%' OR
                CARGO LIKE '%DIRETOR%HELLOO%' OR
                CARGO LIKE '%DIRETOR%ESTRAT%RI%' OR
                CARGO LIKE '%VICE%PRESIDENT%'
                THEN 'DIRETOR EXECUTIVO'
            WHEN CARGO LIKE '%DIRETOR%' THEN 'DIRETOR (N2)'
            WHEN CARGO LIKE '%ESPEC%' OR CARGO LIKE '%ESPECIALISTA%' OR CARGO LIKE '%PRODUCT OWNER%' OR
                CARGO LIKE '%CONSULTOR%' OR CARGO LIKE '%DESENVOLVEDOR%' OR
                CARGO LIKE '%DESIGNER%' OR CARGO LIKE '%ENGENHEIR%DE%DADOS%' OR
                CARGO LIKE '%SECRETARI%' OR CARGO LIKE '%TECH%LEAD%'
                THEN 'ESPECIALISTA'
            WHEN CARGO LIKE '%ESTAGI%' THEN 'ESTAGIÁRIO'
            WHEN CARGO LIKE '%EXECUTIV%' AND CARGO LIKE '%VENDAS%' THEN 'EXECUTIVO DE VENDAS'
            WHEN CARGO LIKE '%GERENTE DE CONTAS A %' THEN 'GERENTE' --GERENTE DE CONTAS A PAGAR OU A RECEBER
            WHEN CARGO LIKE '%GERENTE DE CONTAS%' THEN 'EXECUTIVO DE VENDAS'
            WHEN CARGO LIKE '%GERENTE%' THEN 'GERENTE'
            WHEN CARGO LIKE '%GERENTE%' AND CARGO NOT LIKE 'GERENTE DE CONTAS%' THEN 'GERENTE'
            WHEN CARGO LIKE '%SUPERINTENDENTE%' THEN 'SUPERINTENDENTE'
            WHEN CARGO LIKE '%SUPERVISOR%' THEN 'SUPERVISOR'
            WHEN CARGO LIKE '%ARQUITET%' OR CARGO LIKE '%ENGENHEIR%' THEN 'ARQUITETO & ENGENHEIRO'
            ELSE 'CAMPO'
        END AS GRUPO_CARGO,

        CASE 
            -- ENSINO BÁSICO / FUNDAMENTAL
            WHEN GRAU_DE_INSTRUCAO IN (
                'ENSINO FUNDAMENTAL COMPLETO',
                '5º ANO COMPLETO DO ENSINO FUNDAMENTAL',
                'DO 6º AO 9º ANO DO ENSINO FUNDAMENTAL INCOMPLETO (ANTIGA 5ª A 8ª SÉRIE)',
                'ATÉ O 5º ANO INCOMPLETO DO ENSINO FUNDAMENTAL (ANTIGA 4ª SÉRIE) OU QUE SE TENHA ALFABETIZADO SEM TER FREQUENTADO ESCOLA REGULAR',
                'ANALFABETO, INCLUSIVE O QUE, EMBORA TENHA RECEBIDO INSTRUÇÃO, NÃO SE ALFABETIZOU'
            ) THEN 'ENSINO FUNDAMENTAL'

            -- ENSINO MÉDIO
            WHEN GRAU_DE_INSTRUCAO IN (
                'ENSINO MÉDIO COMPLETO',
                'ENSINO MÉDIO INCOMPLETO',
                'SEGUNDO GRAU TÉCNICO COMPLETO',
                'SEGUNDO GRAU TÉCNICO INCOMPLETO'
            ) THEN 'ENSINO MÉDIO'

            -- ENSINO SUPERIOR (GRADUAÇÃO)
            WHEN GRAU_DE_INSTRUCAO IN (
                'EDUCAÇÃO SUPERIOR COMPLETA',
                'EDUCAÇÃO SUPERIOR INCOMPLETA'
            ) THEN 'ENSINO SUPERIOR'

            -- PÓS-GRADUAÇÃO
            WHEN GRAU_DE_INSTRUCAO LIKE '%P%S%GRADUA%O%' OR GRAU_DE_INSTRUCAO LIKE '%ESPECIALIZA%O%'
            THEN 'PÓS-GRADUAÇÃO'

            -- MESTRADO
            WHEN GRAU_DE_INSTRUCAO LIKE '%MESTRADO%COMPLETO%'
            THEN 'MESTRADO'

            -- DOUTORADO
            WHEN GRAU_DE_INSTRUCAO LIKE '%DOUTORADO%COMPLETO%'
            THEN 'DOUTORADO'

            -- PÓS-DOUTORADO
            WHEN GRAU_DE_INSTRUCAO LIKE 'P%S&DOUTORADO%'
            THEN 'PÓS-DOUTORADO'

            ELSE 'NÃO INFORMADO'
        END AS GRAU_ESCOLARIDADE

    FROM lakehouse_silver.dbo.tb_colaboradores_silver
)

-- Criando a tabela definitiva

SELECT 
    *,
    CASE
        WHEN GRUPO_CARGO IN ('DIRETOR EXECUTIVO', 'DIRETOR (N2)', 'GERENTE', 'SUPERINTENDENTE') THEN 'LÍDER'
        ELSE 'NÃO LÍDER'
    END AS [LIDERANÇA],

    CASE
        WHEN GRUPO_CARGO IN ('DIRETOR EXECUTIVO', 'DIRETOR (N2)', 'GERENTE', 'SUPERINTENDENTE', 'COORDENADOR', 'SUPERVISOR', 'ESPECIALISTA') THEN 'LÍDER'
        ELSE 'NÃO LÍDER'
    END AS [LIDERANÇA_MOVER],

    CASE
        WHEN GRUPO_CARGO = 'CAMPO' THEN 'CAMPO'
        ELSE 'ADMINISTRATIVO'
    END AS CAMPO,

    CASE
        WHEN GRUPO_CARGO = 'ESTAGIÁRIO' OR
        --(CARGO LIKE '%VICE%' AND CARGO LIKE '%PRESIDENT%') OR
        --CARGO LIKE '%DIRETOR%' OR
        CARGO LIKE '%CONSELHEIR%' OR
        GRUPO_CARGO ='APRENDIZ' OR
        [CAUSA_RESCISÃO] LIKE '%CESSÃO%' OR
        [CAUSA_RESCISÃO] LIKE '%TRANSFER%SEM%NUS%'
        --([CAUSA_RESCISÃO] LIKE '%TRANSFER%' AND [CAUSA_RESCISÃO] LIKE '%SEM%' AND [CAUSA_RESCISÃO] LIKE '%NUS%')
        THEN 0
        ELSE 1
    END AS REGRA_ATIVOS_TURNOVER,

    CASE
        WHEN GRUPO_CARGO = 'ESTAGIÁRIO' OR
        --(CARGO LIKE '%VICE%' AND CARGO LIKE '%PRESIDENT%') OR
        --CARGO LIKE '%DIRETOR%' OR
        CARGO LIKE '%CONSELHEIR%' OR
        GRUPO_CARGO ='APRENDIZ' OR
        [CAUSA_RESCISÃO] LIKE '%CESSÃO%' OR
        [CAUSA_RESCISÃO] LIKE '%TRANSFER%SEM%NUS%'
        --([CAUSA_RESCISÃO] LIKE '%TRANSFER%' AND [CAUSA_RESCISÃO] LIKE '%SEM%' AND [CAUSA_RESCISÃO] LIKE '%NUS%')
        THEN 0
        WHEN DEMISSAO IS NULL AND FILTRO_TRANSFERÊNCIA = 1 THEN 1
        ELSE 1
    END AS REGRA_DEMITIDOS_TURNOVER,
                                                                    
    CASE
        WHEN [CAUSA_RESCISÃO] LIKE '1 -%' OR [CAUSA_RESCISÃO] LIKE '2 -%'OR [CAUSA_RESCISÃO] LIKE '17 -%' OR
            [CAUSA_RESCISÃO] LIKE '8 -%' OR [CAUSA_RESCISÃO] LIKE '25 -%'
        THEN 'INVOLUNTÁRIO'
        WHEN [CAUSA_RESCISÃO] LIKE '27 -%' OR [CAUSA_RESCISÃO] LIKE '30 -%' OR [CAUSA_RESCISÃO] LIKE '4 -%'
        THEN 'VOLUNTÁRIO'
        WHEN  [CAUSA_RESCISÃO] LIKE '%CESSÃO%' OR
              [CAUSA_RESCISÃO] LIKE '%TRANSFER%SEM%NUS%' OR [CAUSA_RESCISÃO] LIKE '5 -%'
        THEN 'TRANSFERÊNCIA'
        WHEN [CAUSA_RESCISÃO] = '' THEN ''
        ELSE 'AJUSTAR LINHA'
    END AS MOTIVO_TURNOVER,

    CASE
        WHEN GRUPO_CARGO = 'CEO' THEN 1
        WHEN GRUPO_CARGO = 'DIRETOR EXECUTIVO' THEN 2
        WHEN GRUPO_CARGO = 'DIRETOR (N2)' THEN 3
        WHEN GRUPO_CARGO = 'SUPERINTENDENTE' THEN 4
        WHEN GRUPO_CARGO = 'GERENTE' THEN 5
        WHEN GRUPO_CARGO = 'COORDENADOR' THEN 6
        WHEN GRUPO_CARGO = 'SUPERVISOR' THEN 7
        WHEN GRUPO_CARGO = 'ESPECIALISTA' THEN 8
        WHEN GRUPO_CARGO = 'ANALISTA' THEN 9
        ELSE 10
    END AS INDICE_GRUPO_CARGO,

CASE
    WHEN GRAU_ESCOLARIDADE = 'PÓS-DOUTORADO' THEN 1
    WHEN GRAU_ESCOLARIDADE = 'DOUTORADO' THEN 2
    WHEN GRAU_ESCOLARIDADE = 'MESTRADO' THEN 3
    WHEN GRAU_ESCOLARIDADE = 'PÓS-GRADUAÇÃO' THEN 4
    WHEN GRAU_ESCOLARIDADE = 'ENSINO SUPERIOR' THEN 5
    WHEN GRAU_ESCOLARIDADE = 'ENSINO MÉDIO' THEN 6
    WHEN GRAU_ESCOLARIDADE = 'ENSINO FUNDAMENTAL' THEN 7
    ELSE 8
END AS INDICE_GRAU_ESCOLARIDADE,

CASE
    WHEN [Com_qual_cor_ou_raça_você_se_identifica?] IN ('Parda', 'Preta') THEN 'Negra'
    ELSE [Com_qual_cor_ou_raça_você_se_identifica?]
END AS RAÇA_AUTODECLARADA

INTO dbo.tb_colaboradores_staging
FROM CTE_temp
WHERE ADMISSAO < DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1); -- FILTRANDO APENAS OS COLABORADORES DO MÊS DE FECHAMENTO PARA TRÁS

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT DISTINCT [CAUSA_RESCISÃO], MOTIVO_TURNOVER FROM tb_colaboradores_staging ORDER BY MOTIVO_TURNOVER

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT DISTINCT [GRUPO_CARGO] FROM tb_colaboradores_staging ORDER BY [GRUPO_CARGO]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT COUNT(*) FROM tb_colaboradores_staging WHERE
É_HELLOO IS NULL
AND É_SHOPPING_ADM IS NULL
AND DEMISSAO IS NULL

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT DISTINCT DIRETORIA FROM tb_colaboradores_staging WHERE DEMISSAO IS NULL

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT * FROM tb_colaboradores_staging

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
