# Databricks notebook source
# DBTITLE 1,Carrega conexão com o banco de dados - Python
# MAGIC %run
# MAGIC 
# MAGIC ./../Connections/Connection_DB

# COMMAND ----------

# DBTITLE 1,Carrega parametros do ADF
dbutils.widgets.text("tbl_origem", "")
dbutils.widgets.text("tbl_destino", "")

tbl_origem = dbutils.widgets.get("tbl_origem")
tbl_destino = dbutils.widgets.get("tbl_destino")

print("tabela origem: ", tbl_origem)
print("tabela destino: ", tbl_destino)

# COMMAND ----------

# DBTITLE 1,Lê dados da tabela de origem
origemDF = spark.table(tbl_origem)

# COMMAND ----------

display(origemDF.limit(5))

# COMMAND ----------

origemRmColunasDF = (origemDF
                     .drop("Insert_Date")
                     .drop("Updated_Date")
)

# COMMAND ----------

display(origemRmColunasDF.limit(5))

# COMMAND ----------

write_db_owerwrite(origemRmColunasDF, tbl_destino)

# COMMAND ----------

destinodbDF = read_db_query(f"SELECT TOP 10 * FROM {tbl_destino}")

display(destinodbDF)
