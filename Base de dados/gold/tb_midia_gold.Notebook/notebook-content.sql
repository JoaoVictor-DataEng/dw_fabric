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

IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'tb_midia')
BEGIN
    DROP TABLE dbo.tb_midia;
END;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE TABLE dbo.tb_midia AS
    SELECT 
        *,
        CASE 
            WHEN [Status vaga] = 'ABERTA' THEN 1
            WHEN [Status vaga] = 'CANCELADA' OR [Status vaga] = 'FECHADA' THEN 2
            WHEN [Status vaga] = 'EM ANDAMENTO' THEN 3
            WHEN [Status vaga] = 'CONGELADA' THEN 4
            ELSE 'VERFIFICAR REGRA'
        END AS [Status_ID]
    FROM lakehouse_bronze.dbo.tb_midia

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT DISTINCT Status_ID FROM tb_midia

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
