# Databricks notebook source
# DBTITLE 1,#Accessing the Azure data lake repo
acc_key = dbutils.secrets.get(scope="interactiveDV", key="accessKey")
spark.conf.set("fs.azure.account.key.interactive0d0v.dfs.core.windows.net", acc_key)


# COMMAND ----------

# DBTITLE 1,Reading the csv
df=spark.read.format('csv').option('header', "True").load('abfss://covid-semi-cleaned@interactive0d0v.dfs.core.windows.net/part-00000-bb309251-8451-45a2-b80c-675daa9befc6-c000.csv')

# COMMAND ----------

# DBTITLE 1,Removing spaces from column name
from pyspark.sql.functions import col, cast, lit

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
df = df.dropDuplicates(['State'])


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

# DBTITLE 1,Total Vaccine Distribution State Wise

total_vaccinated_df = df.groupBy('State').agg(
    sum('Total_Individuals_Vaccinated').alias('Total_doses')
)

display(total_vaccinated_df)


# COMMAND ----------

# DBTITLE 1,Getting total population of states from external source so that we can calculate statewise total vaccinated percentage
total_population=dict()
total_population['Uttar Pradesh'] = 199812341
total_population['Maharashtra'] = 112374333
total_population['Bihar'] = 104099452
total_population['West Bengal'] = 91276115
total_population['Madhya Pradesh'] = 72626809
total_population['Tamil Nadu'] = 72147030
total_population['Rajasthan'] = 68548437
total_population['Karnataka'] = 61095297
total_population['Gujarat'] = 60439692
total_population['Andhra Pradesh'] = 49577103
total_population['Odisha'] = 41974219
total_population['Telangana'] = 35003674
total_population['Kerala'] = 33406061
total_population['Jharkhand'] = 32988134
total_population['Assam'] = 31205576
total_population['Punjab'] = 27743338
total_population['Chhattisgarh'] = 25545198
total_population['Haryana'] = 25351462
total_population['NCT of Delhi'] = 16787941
total_population['Jammu and Kashmir'] = 12267032
total_population['Uttarakhand'] = 10086292
total_population['Himachal Pradesh'] = 6864602
total_population['Tripura'] = 3673917
total_population['Meghalaya'] = 2966889
total_population['Manipur'] = 2570390
total_population['Nagaland'] = 1978502
total_population['Goa'] = 1458545
total_population['Arunachal Pradesh'] = 1383727
total_population['Puducherry'] = 1247953
total_population['Mizoram'] = 1097206
total_population['Chandigarh'] = 1055450
total_population['Sikkim'] = 610577
total_population['Dadra and Nagar Haveli and Daman and Diu'] = 585764
total_population['Andaman and Nicobar Islands'] = 380581
total_population['Ladakh'] = 274000
total_population['Lakshadweep'] = 64473
total_population['India'] = 1630569573


# COMMAND ----------

# DBTITLE 1,Vaccinated Percentage in each state
from pyspark.sql.functions import transform, udf, cast
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def get_state_total_population(state):
    return total_population[state] if state in total_population.keys() else None

state_percent_df = total_vaccinated_df.select(\
    total_vaccinated_df.State,\
    total_vaccinated_df.Total_doses,\
    get_state_total_population( total_vaccinated_df.State ).alias('Total Population')
    )

display( state_percent_df )


# COMMAND ----------


