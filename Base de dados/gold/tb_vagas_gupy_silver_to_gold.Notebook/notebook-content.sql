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

IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'tb_vagas_gupy')
BEGIN
    DROP TABLE dbo.tb_vagas_gupy;
END;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************


CREATE TABLE tb_vagas_gupy AS
    SELECT 
    *,
    YEAR([Data_de_Publicação]) AS [Ano_publicação_vaga],
    CONCAT(MONTH([Data_de_Publicação]), '\', YEAR([Data_de_Publicação])) AS [Mês_publicação_vaga],

    CASE
        WHEN Recrutador_da_vaga
            IN ('ANGELO ANTONIO DE MORAIS SOUSA', 'CASSIA LAPAS', 'ESTER MARTINS DA SILVA', 'JACQUELINE CANDIDA DE JESUS',
                'JOAO MATEUS CANDIDO', 'KAMILLA DE BARROS BATISTA PEREIRA', 'KELLEN DE AGUIAR SILVA', 'PATRICK ANTONIO DA COSTA',
                'RENATA SOUZA COSTA', 'ROSANA DE SOUZA VINCO')
            THEN 'Time atração'
        WHEN Recrutador_da_vaga
            IN ('ELIANE DOMINGUES', 'ESTELA', 'KÁTIA ALVES DE MELO', 'LYZANDRA RODRIGUES', 'MARLI APARECIDA', 'POLLYANA RIBEIRO')
            THEN 'Time administrados'
            ELSE 'Time shopping'
    END AS Grupo_recrutador

    FROM lakehouse_silver.dbo.tb_vagas_gupy_silver

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT * FROM tb_vagas_gupy

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
