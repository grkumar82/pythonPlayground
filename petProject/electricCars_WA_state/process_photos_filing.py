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


class Process_EV_data:
    @staticmethod
    def spark_config():
        """
        :return: returns spark config
        """
        spark_conf = (
            SparkConf()
            .setAppName("Electric_Cars_Analysis")
            .setMaster("local[*]")
            .set("spark.driver.memory", "1g")
            .set("spark.executor.memory", "2g")
            .set("spark.executor.cores", "2")
            .set("spark.default.parallelism", "4")
            .set(
                "spark.executor.extraJavaOptions",
                "-XX:+UseG1GC -XX:MaxGCPauseMillis=100",
            )
            .set(
                "spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=100"
            )
        )
        return spark_conf

    @staticmethod
    def spark_schema():
        """
        :return: Spark schema
        """
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
        return schema

    def __init__(self):
        self.input_file = INPUT_FILE
        (
            self.vin_index,
            self.model_year_index,
            self.make_index,
            self.model_index,
            self.ev_type_index,
            self.electric_range_index,
        ) = (0, 0, 0, 0, 0, 0)

        self.column_names = {}

        spark_conf = self.spark_config()
        # Create a SparkSession with the given configuration
        self.spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        self.spark_schema = self.spark_schema()
        (
            self.raw_df,
            self.count_cars_df,
            self.top_3_cars_df,
            self.fastest_growing_car_df,
        ) = (
            self.spark.createDataFrame([], schema=self.spark_schema),
            self.spark.createDataFrame([], schema=self.spark_schema),
            self.spark.createDataFrame([], schema=self.spark_schema),
            self.spark.createDataFrame([], schema=self.spark_schema),
        )

    def columns_index(self, input_dict):
        """
        :param input_dict: dict
        :return: tuple
        takes columns dict and returns columns index
        """
        for key, val in enumerate(input_dict):
            column_name = val["fieldName"]
            if column_name == "vin_1_10":
                self.vin_index = key
            elif column_name == "model_year":
                self.model_year_index = key
            elif column_name == "make":
                self.make_index = key
            elif column_name == "model":
                self.model_index = key
            elif column_name == "ev_type":
                self.ev_type_index = key
            elif column_name == "electric_range":
                self.electric_range_index = key

    def add_values_to_df(self, input_data):
        """
        :param input_data: dict
        :return: Spark dataframe
        Gets data from the input dataframe
        """
        # Create an empty schema for the dataframe
        df = self.spark.createDataFrame([], schema=self.spark_schema)
        # hard coding to limit to 1K rows due to limited computed processing of local machine
        for i in range(len(input_data)):
            vin = input_data[i][self.vin_index]
            model_year = str(input_data[i][self.model_year_index])
            make = str(input_data[i][self.make_index])
            model = input_data[i][self.model_index]
            ev_type = input_data[i][self.ev_type_index]
            electric_range = str(input_data[i][self.electric_range_index])
            row = (vin, model_year, make, model, ev_type, electric_range)
            new_row = self.spark.createDataFrame([row], schema=self.spark_schema)
            df = df.union(new_row)
        return df

    def process_input_file(self):
        with open(self.input_file) as f:
            data = json.load(f)
        column = data["meta"]["view"]["columns"]
        self.columns_index(column)
        self.raw_df = self.add_values_to_df(data["data"])

    def count_cars(self):
        """
        :return: None
        Count of electric cars by model
        """
        self.count_cars_df = (
            self.raw_df.groupBy("model")
            .agg(count("*").alias("count_cars"))
            .orderBy(col("count_cars").desc())
        )
        self.count_cars_df.show()

    def top_three_cars(self):
        """
        :return: None
        # Top 3 electric cars (by registration) in WA in years 2018 - 202
        """
        count_cars = self.raw_df.groupBy("model_year", "model").agg(
            countDistinct("vin").alias("count_cars")
        )

        w = Window.partitionBy("model_year").orderBy(col("count_cars").desc())

        self.top_3_cars_df = (
            count_cars.withColumn("rank_of_car", rank().over(w))
            .filter(
                (col("rank_of_car") <= 3) & (col("model_year").cast("bigint") >= 2018)
            )
            .orderBy(col("model_year").desc(), col("rank_of_car").asc())
            .select("model_year", "model", "rank_of_car")
        )
        self.top_3_cars_df.show()

    def fastest_growing_car(self):
        """
        :return: None
        # Which electric car saw the fastest increase in registrations in 2022 compared to 2021
        """
        count_cars = self.raw_df.groupBy("model_year", "model").agg(
            countDistinct("vin").alias("cnt_cars")
        )

        window = Window.partitionBy("model").orderBy("model_year")
        cars_registration_increase = count_cars.withColumn(
            "car_count_delta", col("cnt_cars") - lag(col("cnt_cars")).over(window)
        ).filter(col("model_year").isin("2021", "2022"))

        self.fastest_growing_car_df = (
            cars_registration_increase.orderBy(col("car_count_delta").desc())
            .limit(1)
            .select("model")
        )
        # Show the result
        self.fastest_growing_car_df.show()

    # Stop the SparkSession
    def stop_spark_session(self):
        self.spark.stop()


test_run = Process_EV_data()
test_run.process_input_file()
test_run.count_cars()
test_run.top_three_cars()
test_run.fastest_growing_car()
test_run.stop_spark_session()

# Outputs when limited to only 100 rows due to limited processing power on my laptop
# count_cars_df
+--------------+----------+
|         model|count_cars|
+--------------+----------+
|       MODEL 3|       205|
|       MODEL Y|       148|
|          LEAF|       145|
|       MODEL S|        58|
|       BOLT EV|        49|
|          VOLT|        46|
|       MODEL X|        29|
|          NIRO|        24|
|         C-MAX|        18|
|      PACIFICA|        17|
|MUSTANG MACH-E|        17|
|   PRIUS PRIME|        16|
|          ID.4|        16|
|        FUSION|        14|
|      WRANGLER|        14|
|          XC90|        13|
|            X5|        11|
|      BOLT EUV|         9|
|           EV6|         9|
|            I3|         8|
+--------------+----------+

# top_3_cars_df
+----------+--------------+-----------+
|model_year|         model|rank_of_car|
+----------+--------------+-----------+
|      2023|       MODEL Y|          1|
|      2023|      BOLT EUV|          2|
|      2023|       MODEL 3|          3|
|      2023|          ID.4|          3|
|      2022|       MODEL Y|          1|
|      2022|       MODEL 3|          2|
|      2022|           EV6|          3|
|      2022|          LEAF|          3|
|      2021|       MODEL Y|          1|
|      2021|       MODEL 3|          2|
|      2021|MUSTANG MACH-E|          3|
|      2020|       MODEL 3|          1|
|      2020|          LEAF|          2|
|      2020|       MODEL Y|          2|
|      2019|       MODEL 3|          1|
|      2019|          LEAF|          2|
|      2019|       BOLT EV|          3|
|      2018|       MODEL 3|          1|
|      2018|       MODEL S|          2|
|      2018|          LEAF|          3|
+----------+--------------+-----------+

# fastest_growing_car_df 
+-------+
|  model|
+-------+
|MODEL Y|
+-------+
