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

DROP TABLE IF EXISTS dbo.tb_colaboradores;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE TABLE tb_colaboradores AS 
SELECT 
    *,
    YEAR(ADMISSAO) AS ANO_ADMISSAO,
    YEAR(DEMISSAO) AS ANO_DEMISSAO,
    DATEADD(DAY, 90, ADMISSAO) AS FIM_EXPERIENCIA,

    CASE
        WHEN CARGO LIKE '%ESTAGI%RI%'
        OR CARGO LIKE '%JOVEM%' OR CARGO LIKE '%APRENDIZ%' OR CARGO LIKE '%COPEIRA%' OR CARGO LIKE '%RECEPCIONISTA%'
        OR CARGO LIKE '%SECRET%RIA%' OR CARGO LIKE '%TRAINEE%' OR CARGO LIKE '%CONSELH%' OR CARGO LIKE '%SUPERVISOR%'
        THEN 0
        ELSE 1
    END AS REGRA_DIF_SALARIAL,

    CONCAT(
        DATEDIFF(MONTH, NASCIMENTO, GETDATE()) / 12, 
        ' Anos e ', 
        DATEDIFF(MONTH, NASCIMENTO, GETDATE()) % 12, 
        ' Meses'
    ) AS IDADE
    
FROM dbo.tb_colaboradores_staging

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

-- Eliminando as colunas desnecessárias

DECLARE @SQL NVARCHAR(MAX) = '';
SELECT @SQL += 'ALTER TABLE dbo.tb_colaboradores DROP COLUMN [' + COLUMN_NAME + '];'
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'tb_colaboradores'
  AND COLUMN_NAME LIKE 'NIVE%';

EXEC sp_executesql @SQL;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT DISTINCT [CAUSA_RESCISÃO], MOTIVO_TURNOVER FROM tb_colaboradores ORDER BY MOTIVO_TURNOVER

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT DISTINCT [GRUPO_CARGO] FROM tb_colaboradores ORDER BY [GRUPO_CARGO]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT COUNT(*) FROM tb_colaboradores
WHERE DEMISSAO IS NULL
AND É_HELLOO IS NULL
AND É_SHOPPING_ADM IS NULL

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
