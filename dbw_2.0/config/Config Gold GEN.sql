-- Databricks notebook source
CREATE DATABASE gold_gen
LOCATION '/mnt/gold/db_gen'

-- COMMAND ----------

CREATE TABLE gold_gen.dim_calendario(
	ANO int
	, MES int
	, DIA int
	, DATA DATE
)
USING DELTA
COMMENT 'dimensão de calendário'
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE gold_gen.dim_artista(
	COD_ARTISTA INT
	, CPF string
	, NOME string
	, DESCRICAO string
    , Insert_Date string
    , Updated_Date string
)
USING DELTA
COMMENT 'dimensão de artistas' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE gold_gen.dim_local(
	COD_LOCAL int
	, NOME string
	, CIDADE string
	, BAIRRO string
	, ESTADO string
    , Insert_Date string
    , Updated_Date string
)
USING DELTA
COMMENT 'dimensão de localização' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE gold_gen.dim_organizador(
	MATRICULA int
	, NOME string
	, NUMERO string
    , Insert_Date string
    , Updated_Date string
)
USING DELTA
COMMENT 'dimensão de organizador' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE gold_gen.fato_evento(
	COD_EVENTO int
	, COD_LOCAL int
	, NOME string
	, DT_EVENTO date
	, HR_EVENTO string
	, COD_ORGANIZADOR int
	, COD_ARTISTA int
	, VALOR_PGTO double
	, DT_PGTO date
    , Insert_Date string
    , Updated_Date string
)
USING DELTA
COMMENT 'fato evento' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');
