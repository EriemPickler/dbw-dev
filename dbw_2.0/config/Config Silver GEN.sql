-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS silver_gen LOCATION '/mnt/silver/db_gen'

-- COMMAND ----------

CREATE TABLE silver_gen.artista(
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
LOCATION '/mnt/silver/db_gen/artista'
COMMENT 'tabela artistas'
TBLPROPERTIES ('orc.compress'='SNAPPY',
               'auto.purge'='true',
               'delta.logRetentionDuration'='interval 1825 days',
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE silver_gen.estilo_musical(
  COD_ESTILO int,
  DESCRICAO string,
  Updated_Date string,
  Insert_Date string,
  Partition_Date int
)
USING DELTA
PARTITIONED BY (Partition_Date)
LOCATION '/mnt/silver/db_gen/estilo_musical'
COMMENT 'tabela de estilos musicais' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE silver_gen.evento(
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
LOCATION '/mnt/silver/db_gen/evento'
COMMENT 'tabela de eventos' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE silver_gen.evento_artista
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
LOCATION '/mnt/silver/db_gen/evento_artista'
COMMENT 'tabela de evento_artista' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE silver_gen.localizacao(
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
LOCATION '/mnt/silver/db_gen/localizacao'
COMMENT 'tabela de localizacao' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE silver_gen.organizador
(
  MATRICULA int,
  NOME string,
  Updated_Date string,
  Insert_Date string,
  Partition_Date int
)
USING DELTA
PARTITIONED BY (Partition_Date)
LOCATION '/mnt/silver/db_gen/organizador'
COMMENT 'tabela de organizador' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');

-- COMMAND ----------

CREATE TABLE silver_gen.telefone(
  COD_TEL int,
  NUMERO string,
  MAT_ORGANIZADOR int,
  Updated_Date string,
  Insert_Date string,
  Partition_Date int
)
USING DELTA
PARTITIONED BY (Partition_Date)
LOCATION '/mnt/silver/db_gen/telefone'
COMMENT 'tabela de telefones' 
TBLPROPERTIES ('orc.compress'='SNAPPY', 
               'auto.purge'='true', 
               'delta.logRetentionDuration'='interval 1825 days', 
               'delta.deletedFileRetentionDuration'='interval 1825 days',
               'overwriteSchema' = 'true');
