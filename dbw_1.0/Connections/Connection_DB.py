# Databricks notebook source
# DBTITLE 1,Variáveis e secrets - Python
dwServerName    = dbutils.secrets.get('scp-kv-prod','secret-db-host')
dwServerPort    = dbutils.secrets.get('scp-kv-prod','secret-db-port')
dwDatabaseName  = dbutils.secrets.get('scp-kv-prod','secret-db-dbname')
dwUserName      = dbutils.secrets.get('scp-kv-prod','secret-db-user')
dwPassword      = dbutils.secrets.get('scp-kv-prod','secret-db-senha')

# COMMAND ----------

# DBTITLE 1,Cria função para leitura de tabela baseada em query
def read_db_query(query):

    server_name = f"jdbc:sqlserver://{dwServerName}:{dwServerPort};database={dwDatabaseName}"

    df = spark.read\
        .format("jdbc")\
        .option("url",server_name)\
        .option("user", dwUserName)\
        .option("password", dwPassword)\
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .option("query", query)\
        .load();
    
    return df

# COMMAND ----------

# DBTITLE 1,Cria função para leitura de tabela
def read_db_table(tbname):
    
    server_name = f"jdbc:sqlserver://{dwServerName}:{dwServerPort};database={dwDatabaseName}"

    df = spark.read\
        .format("jdbc")\
        .option("url",server_name)\
        .option("user", dwUserName)\
        .option("password", dwPassword)\
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .option("dbtable", tbname)\
        .load();
    
    return df

# COMMAND ----------

# DBTITLE 1,Função para escrita no banco de dados (overwrite) destino - Python
def write_db_owerwrite(df, tb_name):
    
    server_name = f"jdbc:sqlserver://{dwServerName}:{dwServerPort};database={dwDatabaseName}"

    df.write\
        .format("jdbc")\
        .option("url", server_name)\
        .option("user", dwUserName)\
        .option("password", dwPassword)\
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .option("truncate", "true")\
        .option("dbtable", tb_name)\
        .mode("overwrite")\
        .save()

# COMMAND ----------

# DBTITLE 1,Função para escrita no banco de dados (append) destino - Python
def write_db_append(df, tb_name):
    
    server_name = f"jdbc:sqlserver://{dwServerName}:{dwServerPort};database={dwDatabaseName}"

    df.write\
        .format("jdbc")\
        .option("url", server_name)\
        .option("user", dwUserName)\
        .option("password", dwPassword)\
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .option("dbtable", tb_name)\
        .mode("append")\
        .save()

# COMMAND ----------

# DBTITLE 1,Leitura de tabela no banco de dados SQL
#query = "DW.FATO_EVENTO"
#read = read_db_table(query)

#write_db_owerwrite(read, query)

# COMMAND ----------

# DBTITLE 1,Exibe dados de um dataframe
#display(read)
