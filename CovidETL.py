# Databricks notebook source
# DBTITLE 1,#Accessing the Azure data lake repo
acc_key = dbutils.secrets.get(scope="interactiveDV", key="accessKey")
spark.conf.set("fs.azure.account.key.interactive0d0v.dfs.core.windows.net", acc_key)


# COMMAND ----------

# DBTITLE 1,Reading the csv
df=spark.read.format('csv').option('header', "True").load('abfss://covid-data-raw@interactive0d0v.dfs.core.windows.net/covid_vaccine_statewise.csv')

# COMMAND ----------

# DBTITLE 1,Displaying Dataframe
display(df)

# COMMAND ----------



# COMMAND ----------


