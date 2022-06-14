-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS silver
LOCATION '/mnt/silver/db/'

-- COMMAND ----------

CREATE TABLE silver.artista(
	COD_ARTISTA int,
	CPF string,
	NOME string,
	COD_ESTILO int,
    Updated_Date string,
    Insert_Date string,
    Partition_Date int
)
USING DELTA
PARTITIONED BY (Partition_Date)
COMMENT 'tabela artistas'
TBLPROPERTIES ('orc.compress'='SNAPPY',
               'auto.purge'='true',
               'delta.logRetentionDuration'='interval 1825 days',
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE silver.estilo_musical(
  COD_ESTILO int,
  DESCRICAO string,
  Updated_Date string,
  Insert_Date string,
  Partition_Date int
)
USING DELTA
PARTITIONED BY (Partition_Date)
COMMENT 'tabela de estilos musicais' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE silver.evento(
  COD_EVENTO int,
  NOME string,
  DATA_EVENTO timestamp,
  FKCOD_LOCAL int,
  FKCOD_ORGANIZADOR int,
  Updated_Date string,
  Insert_Date string,
  Partition_Date int
)
USING DELTA
PARTITIONED BY (Partition_Date)
COMMENT 'tabela de eventos' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE silver.evento_artista
(
  CODIGO int,
  VALOR_PGTO double,
  DT_PGTO timestamp,
  FKCOD_ARTISTA int,
  FKCOD_EVENTO int,
  Updated_Date string,
  Insert_Date string,
  Partition_Date int
)
USING DELTA
PARTITIONED BY (Partition_Date)
COMMENT 'tabela de evento_artista' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE silver.localizacao(
  COD_LOCAL int,
  NOME string,
  CIDADE string,
  BAIRRO string,
  ESTADO string,
  Updated_Date string,
  Insert_Date string,
  Partition_Date int
)
USING DELTA
PARTITIONED BY (Partition_Date)
COMMENT 'tabela de localizacao' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE silver.organizador
(
  MATRICULA int,
  NOME string,
  Updated_Date string,
  Insert_Date string,
  Partition_Date int
)
USING DELTA
PARTITIONED BY (Partition_Date)
COMMENT 'tabela de organizador' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE silver.telefone(
  COD_TEL int,
  NUMERO string,
  MAT_ORGANIZADOR int,
  Updated_Date string,
  Insert_Date string,
  Partition_Date int
)
USING DELTA
PARTITIONED BY (Partition_Date)
COMMENT 'tabela de telefones' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

select * from silver.telefone
