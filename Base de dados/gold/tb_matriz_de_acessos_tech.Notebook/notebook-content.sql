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

IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'tb_matriz_de_acessos_tech')
BEGIN
    DROP TABLE dbo.tb_matriz_de_acessos_tech;
END;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE TABLE tb_matriz_de_acessos_tech AS
    SELECT NOME, EMAIL, CARGO, GRUPO_CARGO, GESTOR, DIRETORIA, UNIDADE
    FROM tb_colaboradores
    WHERE DEMISSAO IS NULL AND [É_HELLOO] IS NULL AND [É_SHOPPING_ADM] IS NULL

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT COUNT(*) FROM tb_matriz_de_acessos_tech

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
