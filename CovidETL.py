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
total_population['Delhi'] = 32941000
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
total_population['Sikkim'] = 700577
total_population['Dadra and Nagar Haveli and Daman and Diu'] = 785764
total_population['Andaman and Nicobar Islands'] = 380581
total_population['Ladakh'] = 274000
total_population['Lakshadweep'] = 75473
total_population['India'] = 1630569573


# COMMAND ----------

# DBTITLE 1,Vaccinated Percentage in each state

from pyspark.sql.functions import transform, udf, cast
from pyspark.sql.types import StringType, FloatType

@udf(returnType=IntegerType())
def get_state_total_population(state):
    return total_population[state] if state in total_population.keys() else None


@udf(returnType=StringType())
def get_percentage(doses, total):
    if total is not None:
        percentage = ( int(doses) * 100 )/total;
        return ("%.2f" % percentage)
    else:
        return None

    

state_without_per_df = total_vaccinated_df.select(\
    total_vaccinated_df.State,\
    total_vaccinated_df.Total_doses,\
    get_state_total_population( total_vaccinated_df.State ).alias('Total_Population'),\
    )

state_without_per_df.withColumn('Total_doses', state_without_per_df.Total_doses.cast('int'))




state_with_per_df = state_without_per_df.select(\
    state_without_per_df.State,\
    state_without_per_df.Total_doses,\
    state_without_per_df.Total_Population,\
    get_percentage( state_without_per_df.Total_doses, state_without_per_df.Total_Population ).alias('Vaccinated_Percentage')

    )

display(state_with_per_df)

state_without_per_df.printSchema()


# COMMAND ----------

#visualization

import matplotlib
import matplotlib.pyplot as plt
from mpl_interactions import ioff, panhandler, zoom_factory
%matplotlib inline 

import matplotlib.ticker as ticker




# COMMAND ----------


total_vaccinated_df_pandas = total_vaccinated_df.toPandas()

total_vaccinated_df_pandas.plot(kind='bar',y='Total_doses',x='State', color = "#4CAF50", width=0.8, title="Number of vaccinated person in each state")

  
fig = plt.figure(figsize = (30, 5))

 
# creating the bar plot


# COMMAND ----------

Total_gender_doses_cleaned = Total_gender_doses.withColumn('Male_Doses', col('Male_Doses'). cast('int'))\
    .withColumn('Female_Doses', col('Female_Doses'). cast('int'))




Total_gender_doses_cleaned_pandas = Total_gender_doses_cleaned.toPandas()

# COMMAND ----------

import numpy as np




N = 3
ind = np.arange(len(Total_gender_doses_cleaned_pandas.State)) 
width = 0.25
plt.rcParams['figure.figsize'] = [20, 10]
bar1 = plt.bar(ind, Total_gender_doses_cleaned_pandas.Male_Doses, width, color = 'r')
bar2 = plt.bar(ind+width, Total_gender_doses_cleaned_pandas.Female_Doses, width, color='g')
plt.xlabel("States")
plt.ylabel('Gender')
plt.title("Gender wise Vaccination ratio in each state")
plt.xticks(ind+width,Total_gender_doses_cleaned_pandas.State)
plt.xticks(rotation=90)

plt.legend( (bar1, bar2), ('Male', 'Female' ) )



plt.show()

# COMMAND ----------

from pyspark.sql.functions import collect_list
vaccine_data_frame_cleaned = vaccine_data_frame.withColumn('Covax', col('Covax').cast('int'))\
    .withColumn('CoviShield', col('CoviShield').cast('int'))

x = [vaccine_data_frame_cleaned.collect()[0][0], vaccine_data_frame_cleaned.collect()[0][1]]
mylabels = ["Covaxin", "Covishield"]
plt.figure(figsize=(8,8))
plt.pie(x, labels = mylabels, autopct='%1.2f%%')

plt.title(
    label="Vaccine Distribution Percentage by Vaccine Type", 
)
plt.show() 




# COMMAND ----------


import pandas as pd
import geopandas as gpd
import shapefile as shp
import seaborn as sns
from shapely.geometry import Point

pd.set_option('display.max_columns', None)  
shp_gdf = gpd.read_file('/dbfs/FileStore/India_State_Boundary.shp')

@udf(returnType=StringType())
def adjust_state_name(state):
    if state == "Andaman and Nicobar Islands":
        return 'Andaman & Nicobar'
    elif state == "Chhattisgarh":
        return 'Chhattishgarh'
    elif state == "Dadra and Nagar Haveli and Daman and Diu":
        return 'Daman and Diu and Dadra and Nagar Haveli'
    elif state == 'Tamil Nadu':
        return 'Tamilnadu'
    elif state == 'Telangana':
        return 'Telengana'
    else:
        return state


state_with_per_up_df = state_with_per_df.withColumn('State', adjust_state_name(state_with_per_df.State))

df = state_with_per_up_df.toPandas()


merged = shp_gdf.set_index('State_Name').join(df.set_index('State'))
merged.head()







# COMMAND ----------

fig, ax = plt.subplots(1, figsize=(12, 12))
ax.axis('off')
ax.set_title('Covid Vaccinated percentage state wise',
             fontdict={'fontsize': '15', 'fontweight' : '3'})
fig = merged.plot(column='Vaccinated_Percentage', cmap='RdYlGn', linewidth=0.5, ax=ax, edgecolor='0.2',legend=True)


# COMMAND ----------


