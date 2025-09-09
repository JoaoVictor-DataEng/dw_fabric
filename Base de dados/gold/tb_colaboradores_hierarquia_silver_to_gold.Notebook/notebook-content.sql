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

DROP TABLE IF EXISTS dbo.tb_colaboradores_hierarquia;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

-- Criando as tabelas temporárias

WITH CTE_COUNT_CPF AS 
    (
        SELECT COUNT(DISTINCT CPF) AS SUBORDINADOS_DIRETOS, CPF_GESTOR
        FROM dbo.tb_colaboradores_staging
        WHERE DEMISSAO IS NULL
        GROUP BY CPF_GESTOR
    ),

CTE_tb_colaboradores_hierarquia AS
    (
        SELECT 
            *
        FROM dbo.tb_colaboradores_staging
    )

-- Criando a tabela definitiva

SELECT 
    *,
    -- CASE
    --     WHEN Unidade = 'Shopping' THEN 'DIRETORIA DE OPERAÇÕES'
    --     WHEN Shopping = 'ALSOTECH' OR CARGO LIKE '%VENDAS DIGITAIS%' THEN 'DIRETORIA DE INOVAÇÃO E TI'
    --     WHEN CARGO LIKE '%CONSELH%' THEN 'CONSELHO'
    --     ELSE DIRETORIA
    -- END AS DIRETORIA,

    CASE
        WHEN GRUPO_CARGO = 'CEO' THEN 1
        WHEN GRUPO_CARGO = 'DIRETOR EXECUTIVO' THEN 2
        WHEN GRUPO_CARGO = 'DIRETOR (N2)' THEN 3
        WHEN GRUPO_CARGO = 'SUPERINTENDENTE' THEN 4
        WHEN GRUPO_CARGO = 'GERENTE' THEN 5
        WHEN GRUPO_CARGO = 'COORDENADOR' THEN 6
        ELSE 10
    END AS INDICE_CARGO,

    CASE
        WHEN FAIXA_GRADE < 0.8 THEN '< 80%'
        WHEN FAIXA_GRADE >= 0.8 AND FAIXA_GRADE < 0.9 THEN '80% - 89%'
        WHEN FAIXA_GRADE >= 0.9 AND FAIXA_GRADE < 1 THEN '90% - 99%'
        WHEN FAIXA_GRADE >= 1 AND FAIXA_GRADE < 1.1 THEN '100% - 109%'
        WHEN FAIXA_GRADE >= 1.1 AND FAIXA_GRADE < 1.2 THEN '110% - 119%'
        WHEN FAIXA_GRADE = 1.2 THEN '120%'
        WHEN FAIXA_GRADE > 1.2 THEN '> 120%'
        ELSE 'Outro'
    END AS [FAIXA_GRADE(grupos)]

INTO tb_colaboradores_hierarquia
FROM CTE_tb_colaboradores_hierarquia
WHERE [É_HELLOO] IS NULL AND [É_SHOPPING_ADM] IS NULL;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse",
-- META   "frozen": false,
-- META   "editable": true
-- META }

-- CELL ********************

-- Criando a versão ALLOS para uso no RLS do Power BI

DROP TABLE IF EXISTS dbo.tb_colaboradores_hierarquia_ALLOS;

CREATE TABLE tb_colaboradores_hierarquia_ALLOS AS
SELECT * FROM  tb_colaboradores_hierarquia

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
