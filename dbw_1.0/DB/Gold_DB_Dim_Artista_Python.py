# Databricks notebook source
# DBTITLE 1,Carrega conexão com o banco de dados - Python
# MAGIC %run
# MAGIC 
# MAGIC ./../Connections/Connection_DB

# COMMAND ----------

# DBTITLE 1,Origem/Destino dos arquivos
#Diretorio de origem
deltaTable_gold = "gold.dim_artista"

#Nome da tabela de destino
tb_name = "DW.DIM_ARTISTA"

# COMMAND ----------

# DBTITLE 1,Leitura dos arquivos da dim_local - Python
dim_artistaDF = spark.table(deltaTable_gold).select("COD_ARTISTA", "CPF", "NOME", "DESCRICAO")

# COMMAND ----------

# DBTITLE 1,Consulta dados do datagrame - Python
display(dim_artistaDF)

# COMMAND ----------

# DBTITLE 1,Carrega os dados na tabela no modo append
'''
obs1: Caso a tabela não exista no schema ela será criada
obs2: no modo append os dados são inseridos em apagar os dados que já existem, correndo o risco de duplicar os dados
'''

write_db_append(dim_artistaDF, tb_name)

# COMMAND ----------

# DBTITLE 1,Carrega os dados na tabela no modo overwrite
'''
obs1: Caso a tabela não exista no schema ela será criada
obs2: Caso a tabela já exista com outra tipagem, a função retornará um erro pois o comando overwrite apenas apaga os dados e reincere, não recria a tabela
'''

write_db_owerwrite(dim_artistaDF, tb_name)

# COMMAND ----------

# DBTITLE 1,Consulta os dados da tabela no banco
display(read_db_table('DW.DIM_ARTISTA'))

# COMMAND ----------

# DBTITLE 1,Consulta os dados da tabela no banco baseada em uma query
display(read_db_query('SELECT count(*) as qtd FROM DW.DIM_ARTISTA'))
