"""
This program is for TASK 3 - redefine tier criteria and save a plot as PNG file (read in README.MD)
"""

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

FLIGHTS = (
    "datasets/flights_2022.csv"
)
TICKETS = (
    "datasets/tickets_2022.csv"
)


class Three_Tiers:
    def __init__(self):
        self.spark = SparkSession.builder.appName("three_tiers").getOrCreate()
        self.flights_df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(FLIGHTS)
        )
        self.tickets_df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(TICKETS)
        )
        self.flights_df.createOrReplaceTempView("flights_table")
        self.tickets_df.createOrReplaceTempView("tickets_table")

    def three_tiers(self):
        result = self.spark.sql(
            f"""
                WITH total_miles AS 
                    (
                    SELECT 
                    A.user_id,
                    SUM(B.distance_miles) AS miles_flown
                    FROM tickets_table A
                    JOIN flights_table B ON A.flight_id = B.flight_id
                    GROUP BY 1
                    ),

                tier_stats AS (
                      SELECT
                        PERCENTILE(miles_flown, 0.9) AS top_10_percentile_miles_flown,
                        PERCENTILE(miles_flown, 0.5) AS top_50_percentile_miles_flown
                      FROM total_miles
                )

                SELECT * FROM tier_stats;
                """
        )
        result_list = result.collect()
        top_10_percentile_miles_flown = round(
            result_list[0]["top_10_percentile_miles_flown"], 2
        )
        top_50_percentile_miles_flown = round(
            result_list[0]["top_50_percentile_miles_flown"], 2
        )
        print(
            f"The number of miles to attain Silver status is {top_50_percentile_miles_flown} miles."
        )
        print(
            f"The number of miles to attain Gold status is {top_10_percentile_miles_flown} miles."
        )
        return [top_10_percentile_miles_flown, top_50_percentile_miles_flown]

    def output_graph(self):
        result = self.spark.sql(
            f"""
                    SELECT 
                    MONTH(B.actual_departure_datetime) AS month,
                    SUM(B.distance_miles) AS miles_flown
                    FROM flights_table B 
                    GROUP BY 1
                    ORDER BY 1
                    """
        )
        # Execute the query and store the results in a pandas dataframe
        result_df = result.toPandas()

        # Convert the pandas dataframe to a numpy array and apply indexing
        x = result_df["month"].values
        y = result_df["miles_flown"].values

        # Create a line plot
        plt.plot(x, y)

        # Add labels and title
        plt.xlabel("Month")
        plt.ylabel("Miles flown")
        plt.title("Total miles flown by month")

        # Save the plot as a PNG file
        plt.savefig("miles_flown.png")
