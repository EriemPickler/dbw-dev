# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Carrega parametros vindos do ADF
dbutils.widgets.text("dir_origem", "")
dbutils.widgets.text("dir_history", "")
dbutils.widgets.text("tbl_destino", "")
dbutils.widgets.text("arq_schema", "")
dbutils.widgets.text("chave_merge", "")

dir_origem = dbutils.widgets.get("dir_origem")
tbl_delta = dbutils.widgets.get("tbl_destino")
arq_schema = dbutils.widgets.get("arq_schema")
dir_history = dbutils.widgets.get("dir_history")
chave_merge = dbutils.widgets.get("chave_merge")

print("origem:", dir_origem)
print("destino:", tbl_delta)
print("history:", dir_history)
print("Schema:", arq_schema)
print("chave merge:", chave_merge)

# COMMAND ----------

chave_merge.split("|")

# COMMAND ----------

# DBTITLE 1,Formata campos para operações futuras
#formata string com diretorio de origem
dir_origem_fmt = "/mnt/" + dir_origem
#formata string com diretorio da history
dir_history_fmt = "/mnt/" + dir_history

schema_fmt = arq_schema

arr_chaves = chave_merge.split("|")
chave_fmt = ""
for chave in arr_chaves:
    if chave != "":
        chave_fmt = f"{chave_fmt} and src.{chave} = dtn.{chave}"

cond_merge = chave_fmt[5:]
print("CHAVE SEM TRATAMENTO:", chave_fmt)
print("diretorio origem formatado:", dir_origem_fmt)
print("diretorio history formatado:", dir_history_fmt)
print("Schema formatado:", schema_fmt)
print("Chave(s) merge:", cond_merge)

# COMMAND ----------

# DBTITLE 1,Carrega arquivo da bronze
origemDF = spark.read.format("csv").option("header", True).option("sep", ";").schema(schema_fmt).load(dir_origem_fmt)

# COMMAND ----------

# DBTITLE 1,Exibe o schema do dataframe
origemDF.printSchema()

# COMMAND ----------

display(origemDF)

# COMMAND ----------

cond_merge

# COMMAND ----------

# DBTITLE 1,Executa merge para atualizar updated_date da tabela na camada silver
deltaDF = DeltaTable.forName(spark, tbl_delta)

deltaDF.alias('dtn')\
    .merge(
        origemDF.alias('src'),
        cond_merge
    )\
    .whenMatchedUpdate(set=
        {
           "dtn.Updated_Date": date_format(current_timestamp()  - expr('INTERVAL 3 HOURS'), 'yyyy/MM/dd HH:mm:ss')
        }
    )\
    .execute()

# COMMAND ----------

origem_novasColunasDF = (origemDF
                         .withColumn("Updated_Date", lit(''))
                         .withColumn("Insert_Date", date_format(current_timestamp() - expr('INTERVAL 3 HOURS'), 'yyyy/MM/dd HH:mm:ss'))
                         .withColumn("Partition_Date", date_format(current_timestamp() - expr('INTERVAL 3 HOURS'), 'yyyyMM').cast("Integer"))
                        )
display(origem_novasColunasDF)

# COMMAND ----------

origem_novasColunasDF.write.mode("append").partitionBy("Partition_Date").saveAsTable(tbl_delta)

# COMMAND ----------

origemDF.write.format("delta").mode("overwrite").save(dir_history_fmt)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from silver_gen.artista
