"""
Queries API and saves results to a json file
"""
# github list of public APIs: https://github.com/public-apis/public-apis#finance
# documentation: https://www.stockdata.org/documentation

import requests
import json
import sys

END_POINT = "https://api.stockdata.org/v1/data/eod"
API_TOKEN = "xxxxx"
PATH_NAME = "tsla_data.json"

params = {
    "api_token": API_TOKEN,
    "symbols": "TSLA",
    "sort": "desc",
    "date_from": "2018-01-01",
    "date_to": "2022-12-31",
}

response = requests.get(END_POINT, params=params)

if response.status_code != 200:
    print(f"Error {response.status_code} while retrieving data from {END_POINT}")
    sys.exit(1)

data = response.json()
with open(PATH_NAME, "w") as f:
    json.dump(data, f)
print(json.dumps(data, indent=4))


"""
This program reads the json data and calculates monthly highest and monthly lowest stock price,
monthly average stock price and monthly average stock volume
"""

import json
import pandas as pd
from collections import defaultdict
import unittest
from statistics import mean

PATH_NAME = "tsla_data.json"

with open(PATH_NAME) as f:
    tsla_df = pd.DataFrame(json.load(f)["data"])

tsla_df["date"] = pd.to_datetime(tsla_df["date"])

# print(tsla_df["date"].dt.month.value_counts().sort_index())

(
    monthly_highest_stock_price,
    monthly_lowest_stock_price,
    monthly_average_stock_price,
    monthly_average_stock_volume,
) = (defaultdict(list), defaultdict(list), defaultdict(list), defaultdict(list))


def calculate_extreme(date_occurred, stock_pr, price_dict, minmax):
    month = date_occurred.month
    prev_date, prev_price = price_dict.get(month, (date_occurred, stock_pr))
    if minmax(prev_price, stock_price) == stock_pr:
        price_dict[month] = (date_occurred, stock_price)


def averages(mon, stock_pr, avg_dict):
    if mon not in avg_dict:
        avg_dict[month].append(stock_pr)
    else:
        avg_dict[month].append(stock_pr)


for index, row in tsla_df.iterrows():
    date_occurence = row["date"]
    month = row["date"].month
    stock_price = row["close"]
    stock_vol = row["volume"]
    # calculate the highest and lowest stock price for a given month
    calculate_extreme(date_occurence, stock_price, monthly_highest_stock_price, max)
    calculate_extreme(date_occurence, stock_price, monthly_lowest_stock_price, min)
    # calculate the avg stock price and volume for a given month
    averages(month, stock_price, monthly_average_stock_price)
    averages(month, stock_vol, monthly_average_stock_volume)


def calculate_averages(avgs_dict, decimals=2):
    return {month: round(mean(units), decimals) for month, units in avgs_dict.items()}


# and use it like:
monthly_avg_price = calculate_averages(monthly_average_stock_price)
monthly_avg_volume = calculate_averages(monthly_average_stock_volume, decimals=0)


class TestMyComputation(unittest.TestCase):
    def test_tsla_df(self):
        self.assertGreater(len(tsla_df), 0)

    def test_monthly_avg(self):
        self.assertEqual(len(monthly_avg_price), 10)
        self.assertEqual(len(monthly_avg_volume), 10)
        self.assertEqual(len(monthly_average_stock_price), 10)
        self.assertEqual(len(monthly_average_stock_volume), 10)


if __name__ == "__main__":
    unittest.main()
