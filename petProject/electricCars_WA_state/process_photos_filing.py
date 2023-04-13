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
from pyspark.sql.functions import coalesce, col, count, countDistinct, lag, rank
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

INPUT_FILE = "electric_vehicles_wa_state.json"

with open(INPUT_FILE) as f:
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
SPARK_CONF = (
    SparkConf()
    .setAppName("Electric_Cars_Analysis")
    .setMaster("local[*]")
    .set("spark.driver.memory", "1g")
    .set("spark.executor.memory", "2g")
    .set("spark.executor.cores", "2")
    .set("spark.default.parallelism", "4")
    .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=100")
    .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=100")
)

# Create a SparkSession with the given configuration
spark = SparkSession.builder.config(conf=SPARK_CONF).getOrCreate()


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
for i in range(1000):
    vin = raw_json_data[i][vin_index]
    model_year = str(raw_json_data[i][model_year_index])
    make = str(raw_json_data[i][make_index])
    model = raw_json_data[i][model_index]
    ev_type = raw_json_data[i][ev_type_index]
    electric_range = str(raw_json_data[i][electric_range_index])
    row = (vin, model_year, make, model, ev_type, electric_range)
    new_row = spark.createDataFrame([row], schema=schema)
    df = df.union(new_row)

# Count of electric cars by model
count_cars_df = (
    df.groupBy("model")
    .agg(count("*").alias("count_cars"))
    .orderBy(col("count_cars").desc())
)
count_cars_df.show()

# Top 3 electric cars (by registration) in WA in years 2018 - 2022
count_cars = df.groupBy("model_year", "model").agg(
    countDistinct("vin").alias("count_cars")
)

w = Window.partitionBy("model_year").orderBy(col("count_cars").desc())

result = (
    count_cars.withColumn("rank_of_car", rank().over(w))
    .filter((col("rank_of_car") <= 3) & (col("model_year").cast("bigint") >= 2018))
    .orderBy(col("model_year").desc(), col("rank_of_car").asc())
    .select("model_year", "model", "rank_of_car")
)

# Which electric car saw the fastest increase in registrations in 2022 compared to 2021
count_cars = df.groupBy("model_year", "model").agg(
    countDistinct("vin").alias("cnt_cars")
)

window = Window.partitionBy("model").orderBy("model_year")
cars_registration_increase = count_cars.withColumn(
    "car_count_delta", col("cnt_cars") - lag(col("cnt_cars")).over(window)
).filter(col("model_year").isin("2021", "2022"))

result = (
    cars_registration_increase.orderBy(col("car_count_delta").desc())
    .limit(1)
    .select("model")
)
# Show the result
result.show()

# Stop the SparkSession
spark.stop()
