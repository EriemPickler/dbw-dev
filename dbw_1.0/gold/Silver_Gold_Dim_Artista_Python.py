# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Origem/Destino dos arquivos
#Diretorio de origem
artistaTbl_silver = "silver.artista"
estilo_musicalTbl_silver = "silver.estilo_musical"

#Diretorio de destino
deltaTable_gold = "gold.dim_artista"

# COMMAND ----------

# DBTITLE 1,Leitura dos arquivos parquet - artistas - Python
#artistaDF = spark.read.format('delta').load(silver_path_artista).filter("Updated_Date = ''")
artistaDF = spark.table(artistaTbl_silver).filter("Updated_Date = ''")

display(artistaDF)

# COMMAND ----------

# DBTITLE 1,Leitura tabela estilos musicais - Python
#estilo_musicalDF = spark.read.format('delta').load(silver_path_estilo_musical).filter("Updated_Date is null")
estilo_musicalDF = spark.table(estilo_musicalTbl_silver).filter("Updated_Date = ''")

display(estilo_musicalDF)

# COMMAND ----------

# DBTITLE 1,Exibir schema - Python
artistaDF.printSchema()

estilo_musicalDF.printSchema()

# COMMAND ----------

# DBTITLE 1,Consultando dados no Dataframe - Python
artistaDF.show()
#ou
display(artistaDF)

# COMMAND ----------

# DBTITLE 1,Top N (Limit) - Python
display(artistaDF.limit(2))

# COMMAND ----------

# DBTITLE 1,Filtro - Python
display(artistaDF.filter(artistaDF.COD_ESTILO == 2))

# COMMAND ----------

# DBTITLE 1,Join - Python
artista_joinDF = artistaDF.join(estilo_musicalDF, artistaDF.COD_ESTILO == estilo_musicalDF.COD_ESTILO, how='inner').select(artistaDF.COD_ARTISTA, artistaDF.CPF, artistaDF.NOME, estilo_musicalDF.DESCRICAO, artistaDF.Insert_Date)

display(artista_joinDF)

# COMMAND ----------

# DBTITLE 1,Inserindo arquivos na gold - Python
dim_artistaDF = DeltaTable.forName(spark, deltaTable_gold)

dim_artistaDF.alias('tb')\
    .merge(
        artista_joinDF.alias('df'),
        'tb.COD_ARTISTA = df.COD_ARTISTA'
    )\
    .whenMatchedUpdate(set=
        {
           "tb.Updated_Date": date_format(current_timestamp(), 'yyyy/MM/dd HH:mm:ss'),
            "tb.NOME": "df.NOME", 
            "tb.DESCRICAO": "df.DESCRICAO"
        }
    )\
    .whenNotMatchedInsert(values=
        {
            "COD_ARTISTA": "df.COD_ARTISTA" ,
            "CPF": "df.CPF" ,
            "NOME": "df.NOME" ,
            "DESCRICAO": "df.DESCRICAO",
            "Insert_Date": "df.Insert_Date"
        }
     )\
    .execute()

# COMMAND ----------

# DBTITLE 1,Consultando dados da tabela delta
display(spark.table('gold.dim_artista'))
