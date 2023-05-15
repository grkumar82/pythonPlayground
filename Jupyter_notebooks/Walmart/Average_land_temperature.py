#!/usr/bin/env python
# coding: utf-8

# In[ ]:


"""
Using the dataset GlobalTemperatures.csv, answer the following questions:
1. What is the maximum average land temperature change per month observed in the global dataset and in which year was it?
In which year did the average land temperatures rise the most according to the dataset?

Answers:
For first question - 
+----------+-----------------+
|        dt|      temp_change|
+----------+-----------------+
|1757-10-01|8.370000000000001|
+----------+-----------------+
For second question - 
+--------+------------------------------+
|year(dt)|completeLandAverageTemperature|
+--------+------------------------------+
|    1761|                        19.021|
+--------+------------------------------+
"""


# In[32]:


from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.functions import *


# In[33]:


globalTemperatures = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("GlobalTemperatures.csv")


# In[34]:


# Collect average temperatures for a given month in a dataset
# and build a dictionary to replace null values to maintain seasonlaity
globalTemperatures.createOrReplaceTempView("source")
result = spark.sql(
    f"""
    SELECT 
        MONTH(dt) as month,
        AVG(LandAverageTemperature) as avgTemp
    FROM source
    WHERE 1=1
    GROUP BY MONTH(dt)
    """
).collect()  
nullSubstituteAvgTempValues = {row.month: row.avgTemp for row in result}
globalTemperatures = globalTemperatures.withColumn("dt", to_date("dt"))
globalTemperatures = globalTemperatures.withColumn("month_value", month(globalTemperatures["dt"]))


# In[35]:


# function to map column value to dictionary key
def get_dict_value(value):
    return nullSubstituteAvgTempValues.get(value, None)

# Register UDF
udf_get_dict_value = udf(get_dict_value)


# In[36]:


# Fill null temperature values with Avg Land temperature in given month in a year
# to maintain the seasonalities
globalTemperatures = globalTemperatures.withColumn("completeLandAverageTemperature", when(
    globalTemperatures["LandAverageTemperature"].isNull(), 
    udf_get_dict_value(col("month_value"))
).otherwise(globalTemperatures["LandAverageTemperature"]))

# Change datatype from String to Double
globalTemperatures = globalTemperatures.withColumn("completeLandAverageTemperature", col("completeLandAverageTemperature").cast("double"))



# In[37]:


print(f"***** Maximum average land temperature change per month *****")
# maximum avg land temperature change per month
globalTemperatures = globalTemperatures.withColumn("temp_change", abs(col("completeLandAverageTemperature") - lag("completeLandAverageTemperature", 1).over(Window.orderBy("dt"))))
globalTemperatures.select(col("dt"), col("temp_change")).orderBy(desc("temp_change")).limit(1).show()

print(f"***** AverageLamdTemparature raise in Year/ Year we saw max LandAverageTemparature *****")
# Store in variable to print
max_temp = globalTemperatures.select(max("completeLandAverageTemperature")).collect()[0][0]
globalTemperatures.select(year(col("dt")), col("completeLandAverageTemperature")).where(col("completeLandAverageTemperature") == max_temp).show()
globalTemperatures.groupBy(year(col("dt"))).agg(avg("completeLandAverageTemperature").alias("avgLandTempPerYear")).orderBy(desc("avgLandTempPerYear")).show()


# In[ ]:




