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

DECLARE @DataInicio DATE = (SELECT MIN(ADMISSAO) FROM tb_colaboradores WHERE ADMISSAO >= '1900-01-01');
DECLARE @DataFim DATE = (SELECT MAX(ADMISSAO) FROM tb_colaboradores);

-- Calcula a quantidade de dias entre as duas datas
DECLARE @QtdDias INT = DATEDIFF(DAY, @DataInicio, @DataFim);

-- Remove a tabela se já existir
IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'tb_calendario')
BEGIN
    DROP TABLE dbo.tb_calendario;
END

-- Cria a tabela com datas contínuas e colunas adicionais
CREATE TABLE dbo.tb_calendario AS
SELECT
    DATEADD(DAY, s.[value], @DataInicio) AS DATA,
    YEAR(DATEADD(DAY, s.[value], @DataInicio)) AS ANO,
    RIGHT('0' + CAST(MONTH(DATEADD(day, s.[value], @DataInicio)) AS VARCHAR), 2)
        + '/' +
    CAST(YEAR(DATEADD(day, s.[value], @DataInicio)) AS VARCHAR) AS MES_ANO,
    DATEFROMPARTS(
        YEAR(DATEADD(DAY, s.[value], @DataInicio)),
        MONTH(DATEADD(DAY, s.[value], @DataInicio)),
        1
    ) AS INICIO_MES
FROM GENERATE_SERIES(0, @QtdDias, 1) AS s;


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
