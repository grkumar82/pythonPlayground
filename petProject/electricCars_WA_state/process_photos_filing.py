"""
This program reads the json file containing electric vehicles in the state of WA and outputs certain metrics in a txt file:
Top 3 electric cars (by registration) in WA in years 2018 - 2022
Count of electric cars by model
Which electric car saw the fastest increase in registrations in 2022 compared to 2021

We will utilize PySpark for this program since the JSON input file size is about 50 MB.
"""
import json

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

input_file = "electric_vehicles_wa_state.json"

with open(input_file) as f:
    data = json.load(f)

column_names = {}
column = data["meta"]["view"]["columns"]

(
    vin_index,
    model_year_index,
    make_index,
    model_index,
    ev_type_index,
    electric_range_index,
) = (0, 0, 0, 0, 0, 0)

for key, val in enumerate(column):
    column_name = val["fieldName"]
    if column_name == "vin_1_10":
        vin_index = key
    elif column_name == "model_year":
        model_year_index = key
    elif column_name == "make":
        make_index = key
    elif column_name == "model":
        model_index = key
    elif column_name == "ev_type":
        ev_type_index = key
    elif column_name == "electric_range":
        electric_range_index = key


# spark config for a local machine
conf = (
    SparkConf()
    .setAppName("MyApp")
    .setMaster("local[*]")
    .set("spark.driver.memory", "1g")
    .set("spark.executor.memory", "2g")
    .set("spark.executor.cores", "2")
    .set("spark.default.parallelism", "4")
    .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=100")
    .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=100")
)

# Create a SparkSession with the given configuration
spark = SparkSession.builder.config(conf=conf).getOrCreate()


schema = StructType(
    [
        StructField("vin", StringType(), True),
        StructField("model_year", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("ev_type", StringType(), True),
        StructField("electric_range", StringType(), True),
    ]
)

# Create an empty schema for the dataframe
df = spark.createDataFrame([], schema=schema)

# section where the data is within the JSON file.
raw_json_data = data["data"]
for i in range(len(raw_json_data)):
    vin = raw_json_data[i][vin_index]
    model_year = str(raw_json_data[i][model_year_index])
    make = str(raw_json_data[i][make_index])
    model = raw_json_data[i][model_index]
    ev_type = raw_json_data[i][ev_type_index]
    electric_range = str(raw_json_data[i][electric_range_index])
    row = (vin, model_year, make, model, ev_type, electric_range)
    new_row = spark.createDataFrame([row], schema=schema)
    df = df.union(new_row)

df.show()

# Stop the SparkSession
spark.stop()
