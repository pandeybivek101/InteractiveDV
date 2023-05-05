# Databricks notebook source
# DBTITLE 1,#Accessing the Azure data lake repo
acc_key = dbutils.secrets.get(scope="interactiveDV", key="accessKey")
spark.conf.set("fs.azure.account.key.interactive0d0v.dfs.core.windows.net", acc_key)


# COMMAND ----------

# DBTITLE 1,Reading the csv
df=spark.read.format('csv').option('header', "True").load('abfss://covid-semi-cleaned@interactive0d0v.dfs.core.windows.net/part-00000-bb309251-8451-45a2-b80c-675daa9befc6-c000.csv')

# COMMAND ----------

# DBTITLE 1,Removing spaces from column name
from pyspark.sql.functions import col, cast

df = df.withColumnRenamed('Updated On', 'Updated_On')
df = df.withColumnRenamed('Total Individuals Vaccinated', 'Total_Individuals_Vaccinated')
df = df.withColumnRenamed(' Covaxin_dose', 'Covaxin_dose')

df = df.withColumn('Total_doses', col('Total_doses').cast('int'))\
        .withColumn('Total_Individuals_Vaccinated', col('Total_Individuals_Vaccinated').cast('int'))\
        .withColumn('Covaxin_dose', col('Covaxin_dose').cast('int'))\
        .withColumn('CoviShield_dose', col('CoviShield_dose').cast('int'))

# COMMAND ----------

# DBTITLE 1,getting data from latest date for each states

from pyspark.sql.functions import date_format, to_date, col
handeled_tottal_vaccine_df = df.withColumn('Total_Individuals_Vaccinated', col('Covaxin_dose')+col('CoviShield_dose'))
handeled_tottal_vaccine_latest_date = handeled_tottal_vaccine_df.tail(73)

df = spark.createDataFrame(handeled_tottal_vaccine_latest_date)


# COMMAND ----------

# DBTITLE 1,Displaying Dataframe
display(df)

# COMMAND ----------

# DBTITLE 1,Filling null values
df = df.fillna(0, ['Covaxin_dose', 'CoviShield_dose'])


# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,Creating data frame for vaccination type recored
from pyspark.sql.functions import udf, sum
from pyspark.sql.types import IntegerType

temp_vaccine = df.select( sum(df.Covaxin_dose).alias('Covax'), sum(df.CoviShield_dose).alias('CoviShield') )

def give_percentage(number, total):
    percentage = (number * 100)/total;
    return ("%.2f" % percentage)

def calculate_percentage(column):
    total = column[0]+column[1]
    covax = give_percentage(column[0], total)
    covi = give_percentage(column[1], total)
    return (covax, covi)

rdd1 = temp_vaccine.rdd.map( lambda column : calculate_percentage(column) )

vaccine_data_frame = rdd1.toDF(['Covax', 'CoviShield'])

vaccine_data_frame.show()



# COMMAND ----------

# DBTITLE 1,Data Frame for total vaccinated by genderwise in each state
df = df.withColumn('Male_dose', col('Male_dose').cast('int')).withColumn('Female_dose', col('Female_dose').cast('int'))

Total_gender_doses = df.groupBy('State').agg(
    sum('Male_dose').alias('Male_Doses'),
    sum('Female_dose').alias("Female_Doses")
)
display(Total_gender_doses)

# COMMAND ----------

# DBTITLE 1,Total Vaccine Distribution Sate Wise

total_vaccinated_df = df.groupBy('State').agg(
    sum('Total_Individuals_Vaccinated').alias('Total_doses')
)

display(total_vaccinated_df)


# COMMAND ----------


