"""
This program is for TASK 1 - calculating percentage of elite members and average number of flights it takes to get to elite status.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

FLIGHTS = (
    "/Users/ravi_k_ganta/PycharmProjects/flights_analysis/src/datasets/flights_2022.csv"
)
MEMBERS = (
    "/Users/ravi_k_ganta/PycharmProjects/flights_analysis/src/datasets/members_2022.csv"
)
ELITE_MEMBER_THRESHOLD = 75000


class Elite_Status:
    def __init__(self):
        self.spark = SparkSession.builder.appName("elite_status").getOrCreate()
        self.members_df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(MEMBERS)
        )
        self.flights_df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(FLIGHTS)
        )
        self.elite_membership_percentage, self.avg_flights_cnt = 0, 0

    def check_null_values(self, df):
        for c in df.columns:
            has_nulls = df.filter(col(c).isNull()).count() > 0
            if has_nulls:
                print(f"Column '{c}' has null values")
                df = df.drop(c)

    def drop_duplicates(self, df):
        dupCount = df.count()
        actCount = df.distinct().count()
        diff = dupCount - actCount
        if dupCount != actCount:
            print(f"Dataframe has duplicates, duplicate count {diff}")
            return df.dropDuplicates()
        else:
            print(f"No duplicates found in the dataset {diff}")
            return df

    def massage_dataframes(self):
        # remove null values
        self.check_null_values(self.members_df)
        self.check_null_values(self.flights_df)
        # remove duplicates
        self.drop_duplicates(self.members_df)
        self.drop_duplicates(self.flights_df)

    def calculate_elite_membership(self):
        self.members_df.createOrReplaceTempView("members_table")
        result = self.spark.sql(
            f"""
            WITH CTE AS 
            (
                SELECT 
                    LOWER(membership_tier) as membership_tier,
                    COUNT(DISTINCT user_id) as tier_cnt
                FROM members_table
                WHERE 1=1
                GROUP BY 1
            )

            SELECT ROUND(E.tier_cnt/tot.total_cnt, 2)*100 AS percentage_elite_members
            FROM CTE E, 
            (
                SELECT SUM(tier_cnt) as total_cnt
                FROM CTE) tot
            WHERE 1=1
            AND E.membership_tier = 'elite'
            """
        ).collect()[0][0]
        # Display the result
        print(f"The percentage of elite members is {result}%")
        self.elite_membership_percentage = int(result)
        return self.elite_membership_percentage

    def calculate_flight_cnt(self, df):
        total_miles = 0
        num_flights = 0
        for row in df.rdd.collect():
            total_miles += row["distance_miles"]
            num_flights += 1
            if total_miles >= ELITE_MEMBER_THRESHOLD:
                break
        return num_flights

    def avg_flights(self):
        desc_sorted_flights_df = self.flights_df.sort(desc("distance_miles"))
        asc_sorted_flights_df = self.flights_df.sort(asc("distance_miles"))
        average_flight = (
            self.calculate_flight_cnt(desc_sorted_flights_df)
            + self.calculate_flight_cnt(asc_sorted_flights_df)
        ) / 2
        print(
            f"Avg number of flights it takes to become a elite member: {average_flight}"
        )
        self.avg_flights_cnt = int(average_flight)
        return self.avg_flights_cnt
