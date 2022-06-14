# Databricks notebook source
# DBTITLE 1,Carrega conexão com o banco de dados - Python
# MAGIC %run
# MAGIC 
# MAGIC ./../Connections/Connection_DB

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Origem/Destino dos arquivos
#Diretorio de origem
gold_path = "/mnt/gold/fato_evento/"

#Nome da tabela de destino
tb_name = "DW.FATO_EVENTO"

# COMMAND ----------

# DBTITLE 1,Leitura dos arquivos da dim_local - Python
fato_eventoDF = spark.table("gold.fato_evento").select("COD_EVENTO", "COD_LOCAL", "NOME", "DT_EVENTO", "HR_EVENTO", "COD_ORGANIZADOR", "COD_ARTISTA", "VALOR_PGTO", "DT_PGTO")

# COMMAND ----------

# DBTITLE 1,Consulta dados do datagrame - Python
display(fato_eventoDF)

# COMMAND ----------

# DBTITLE 1,Carrega os dados na tabela no modo append
'''
obs1: Caso a tabela não exista no schema ela será criada
obs2: no modo append os dados são inseridos em apagar os dados que já existem, correndo o risco de duplicar os dados
'''

write_db_append(fato_eventoDF, tb_name)

# COMMAND ----------

# DBTITLE 1,Carrega os dados na tabela no modo overwrite
'''
obs1: Caso a tabela não exista no schema ela será criada
obs2: Caso a tabela já exista com outra tipagem, a função retornará um erro pois o comando overwrite apenas apaga os dados e reincere, não recria a tabela
'''

write_db_owerwrite(fato_eventoDF, tb_name)

# COMMAND ----------

# DBTITLE 1,Consulta os dados da tabela no banco
display(read_db_table('DW.FATO_EVENTO'))

# COMMAND ----------

# DBTITLE 1,Consulta os dados da tabela no banco baseada em uma query
display(read_db_query('SELECT TOP 1 * FROM DW.FATO_EVENTO'))

# COMMAND ----------

display(fato_eventoDF.withColumn('id_spark', monotonically_increasing_id()))
