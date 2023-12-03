# Databricks notebook source
dbutils.fs.mount(
    source="wasbs://championshiptable@adfdemoblobjsn1971.blob.core.windows.net/",
    mount_point="/mnt/championshiptable",
    extra_configs={"fs.azure.account.key.adfdemoblobjsn1971.blob.core.windows.net":"InFjA+M+PRJ0pf56FTEib5/Ccpocz4rCV11rjy5zWhck/sGWPA9btWQeuCGs8d2XW53qPzAAZiIm+AStJkVwng=="}
)

# COMMAND ----------

df = spark.read.format('csv').options(header='true').load('/mnt/championshiptable/2023/11/05/engchampcurrenttable.csv')
display(df)

# COMMAND ----------

df = df.drop('Last 6')
display(df)

# COMMAND ----------

df_pandas = df.toPandas()
df_pandas.insert(0,'Year',2023)
df_pandas.insert(1,'Month',11)
df_pandas.insert(2,'Day',5)
display(df_pandas)

# COMMAND ----------

df = spark.createDataFrame(df_pandas)
table_name="engchampionshiptable"
df.write.saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from engchampionshiptable

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table engchampionshiptable

# COMMAND ----------

def readContentsAndSaveInTable(expPath,file):
    display("File to export " + expPath)
    df = spark.read.format('csv').options(header='true').load(expPath)
    #display(df) 
    file_date = expPath.replace('dbfs:/mnt/championshiptable','').replace(file,'')[1:-1]
    display(file_date)
    
    # drop the 'Last 6' column, as it doesn't contain meaningful data
    df = df.drop('Last 6')

    df_pandas = df.toPandas()
    df_pandas.insert(0, 'TableDate', file_date)
    #display(df_pandas)

    df = spark.createDataFrame(df_pandas)
    table_name="engchampionshiptable"
    df.write.mode("append").saveAsTable(table_name)    




def exportEngChampFiles(path):
    dir_paths = dbutils.fs.ls(path)
    for p in dir_paths:
        if(p.isDir()):
            display(p.path + "is dir")
            exportEngChampFiles(p.path)
        else:
            readContentsAndSaveInTable(p.path, p.name)




exportEngChampFiles('/mnt/championshiptable/2023')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from engchampionshiptable order by TableDate
