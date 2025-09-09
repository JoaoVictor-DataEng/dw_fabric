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

IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'tb_estrutura_newco')
BEGIN
    DROP TABLE dbo.tb_estrutura_newco;
END;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE TABLE dbo.tb_estrutura_newco AS SELECT * FROM lakehouse_bronze.dbo.tb_estrutura_newco

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
