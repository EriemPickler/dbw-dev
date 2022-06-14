# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
import pyspark.sql.functions as F
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Destino dos arquivos
deltaTable_gold = "gold.dim_calendario"

# COMMAND ----------

# DBTITLE 1,Cria dataframe com o range de data especificado
calendarioDF = spark.createDataFrame([(1,)], ["id"])

calendarioDF = calendarioDF.withColumn(
    "date", 
    F.explode(F.expr("sequence(to_date('2020-01-01'), to_date('2022-12-31'), interval 1 day)"))
)

# COMMAND ----------

# DBTITLE 1,Consultando dados do dataframe
calendarioDF.show()
#ou
display(calendarioDF)

# COMMAND ----------

# DBTITLE 1,Adiciona colunas formatadas no dataframe
calendarioDF = calendarioDF.drop('id')
calendarioDF = calendarioDF.withColumn('Ano', F.date_format(F.col('date'), 'yyyy').cast(IntegerType()))
calendarioDF = calendarioDF.withColumn('Mes', F.date_format(F.col('date'), 'MM').cast(IntegerType()))
calendarioDF = calendarioDF.withColumn('Dia', F.date_format(F.col('date'), 'dd').cast(IntegerType()))
calendarioDF = calendarioDF.withColumn('Data', F.col('date'))
calendarioDF = calendarioDF.drop('date')

# COMMAND ----------

# DBTITLE 1,Consultando dados do dataframe
calendarioDF.show()
#ou
display(calendarioDF)

# COMMAND ----------

# DBTITLE 1,Inserindo arquivos na trusted - Python
calendarioDF.write.mode('overwrite').format('delta').saveAsTable(deltaTable_gold)

# COMMAND ----------

# DBTITLE 1,Consultando arquivos da trusted - Python
dim_calendarioDF = spark.table(deltaTable_gold)

display(dim_calendarioDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from gold.dim_calendario
