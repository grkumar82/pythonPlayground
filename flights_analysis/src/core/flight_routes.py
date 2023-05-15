"""
This program is for TASK 2 - reformat file and find some specific flights (read in README.MD)
"""

from pyspark.sql import SparkSession

FLIGHTS = (
    "/Users/ravi_k_ganta/PycharmProjects/flights_analysis/src/datasets/flights_2022.csv"
)


class Flight_Routes:
    def __init__(self):
        self.spark = SparkSession.builder.appName("flight_routes").getOrCreate()
        self.flights_df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(FLIGHTS)
        )
        self.flights_df.createOrReplaceTempView("flights_table")

    def identify_daily_duplicate_flights(self):
        result = self.spark.sql(
            f"""
                WITH CTE AS 
                (
                    SELECT 
                    departure_airport,
                    arrival_airport,
                    DATE(actual_departure_datetime) AS dt,
                    COUNT(DISTINCT flight_id) AS cnt
                    FROM flights_table
                    WHERE 1=1
                    GROUP BY 1,2,3
                )

                SELECT
                    departure_airport,
                    arrival_airport
                FROM CTE 
                WHERE cnt = 2
                GROUP BY 1,2
            """
        )
        result_list = [str(row[0]) for row in result.collect()]
        result.createOrReplaceTempView("daily_duplicates")
        return result_list

    def identify_weekly_duplicate_flights(self):
        result = self.spark.sql(
            f"""
            WITH CTE AS 
            (
                SELECT 
                departure_airport,
                arrival_airport,
                DATE(date_trunc('week', actual_departure_datetime)) AS start_of_week,
                COUNT(DISTINCT flight_id) AS cnt
                FROM flights_table
                WHERE 1=1
                AND YEAR(actual_departure_datetime) = 2022
                GROUP BY 1,2,3
            )

            SELECT
            departure_airport,
            arrival_airport
            FROM
            (
                SELECT 
                start_of_week,
                departure_airport,
                arrival_airport,
                cnt,
                RANK () OVER (PARTITION BY departure_airport,arrival_airport  ORDER BY start_of_week) as rnk
                FROM CTE 
            ) temp
            WHERE cnt = 2
            AND rnk > 2
            GROUP BY 1,2
            """
        )
        result_list = [str(row[0]) for row in result.collect()]
        result.createOrReplaceTempView("weekly_duplicates")
        return result_list

    def write_to_csv(self):
        result = self.spark.sql(
            f"""
            WITH all_duplicates AS 
            (
                SELECT * FROM weekly_duplicates
                UNION ALL
                SELECT * FROM daily_duplicates
            ),
            
            remove_duplicates AS
            (
            SELECT 
                A.departure_airport,
                A.arrival_airport,
                A.actual_departure_datetime,
                A.actual_arrival_datetime,
                A.distance_miles
            FROM flights_table A
            LEFT JOIN all_duplicates B ON A.departure_airport = B.departure_airport AND A.arrival_airport = B.arrival_airport
            WHERE 1=1
            AND B.departure_airport IS NULL AND B.arrival_airport IS NULL
            GROUP BY 1,2,3,4,5
            )
            
            SELECT
            departure_airport,
            arrival_airport,
            date_format(actual_departure_datetime, 'EEE') AS departure_weekday,
            from_unixtime(UNIX_TIMESTAMP(actual_departure_datetime), 'HH:mm:ss') as departure_time,
            from_unixtime(UNIX_TIMESTAMP(actual_arrival_datetime), 'HH:mm:ss') as arrival_time,
            distance_miles
            FROM remove_duplicates
            """
        )
        # set the number of partitions to 1 before writing
        result.coalesce(1).write.csv("output.csv")
