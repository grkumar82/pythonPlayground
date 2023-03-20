'''
Queries API and saves results to a json file
'''
# github list of public APIs: https://github.com/public-apis/public-apis#finance
# documentation: https://www.stockdata.org/documentation

import requests
import json

END_POINT = "https://api.stockdata.org/v1/data/eod"

params = {
    'api_token': "xxx",
    'symbols': "TSLA",
    'sort': "desc",
    'date_from': "2018-01-01",
    'date_to': "2022-12-31",
}

response = requests.get(END_POINT, params=params)

if response.status_code == 200:
    data = response.json()
    print(json.dumps(data, indent=4))  # Print formatted json
else:
    print('Error:', response.status_code)

with open('tsla_data.json', 'w') as f:
    json.dump(data, f)

    
'''
This program reads the json data and calculates monthly highest and monthly lowest stock price,
monthly average stock price and monthly average stock volume
'''

import json
import pandas as pd
from collections import defaultdict
import unittest

with open('tsla_data.json') as f:
    data = json.load(f)
    stock = data['meta']['ticker']
    num_records = data['meta']['found']
    records = data['data']

# columns for the json file
columns = {"date": "object", "open": float, "high": float, "low": float, "close": float, "volume": int}

tsla_df = pd.DataFrame(records, columns=columns)

tsla_df['date'] = pd.to_datetime(tsla_df['date'], dayfirst=True)

monthly_highest_stock_price, monthly_lowest_stock_price, monthly_average_stock_price, monthly_average_stock_volume = defaultdict(
    list), defaultdict(list), defaultdict(list), defaultdict(list)


def calculate_highest_lowest(date_occurred, stock_pr, mon, price_dict, cal_type):
    if mon not in price_dict:
        price_dict[month].extend([date_occurred, stock_pr])
    if cal_type == 'highest':
        if price_dict[month][1] < stock_pr:
            price_dict[month].pop()
            price_dict[month].pop()
            price_dict[month].extend([date_occurred, stock_pr])
    if cal_type == 'lowest':
        if price_dict[month][1] > stock_pr:
            price_dict[month].pop()
            price_dict[month].pop()
            price_dict[month].extend([date_occurred, stock_pr])


def averages(mon, stock_pr, avg_dict):
    if mon not in avg_dict:
        avg_dict[month].append(stock_pr)
    else:
        avg_dict[month].append(stock_pr)


for index, row in tsla_df.iterrows():
    date_occurence = row['date']
    month = row['date'].month
    stock_price = row['close']
    stock_vol = row['volume']
    # calculate the highest stock price for a given month and the corresponding date
    calculate_highest_lowest(date_occurence, stock_price, month, monthly_highest_stock_price, 'highest')
    # calculate the lowest stock price for a given month and the corresponding date
    calculate_highest_lowest(date_occurence, stock_price, month, monthly_lowest_stock_price, 'lowest')
    # calculate the avg stock price for a given month
    averages(month, stock_price, monthly_average_stock_price)
    averages(month, stock_vol, monthly_average_stock_volume)

monthly_avg_price, monthly_avg_volume = defaultdict(int), defaultdict(int)


def calculate_averages(avgs_dict, data_dict):

    for month, units in avgs_dict.items():
        sum_prices, cnt = 0, 0
        for i in range(len(units)):
            sum_prices += units[i]
            cnt += 1
        data_dict[month] = round(sum_prices / cnt, 2)


calculate_averages(monthly_average_stock_price, monthly_avg_price)
calculate_averages(monthly_average_stock_volume, monthly_avg_volume)


class TestMyComputation(unittest.TestCase):

    def test_tsla_df(self):
        self.assertGreater(len(tsla_df), 0)

    def test_monthly_avg(self):
        self.assertEqual(len(monthly_avg_price), 10)
        self.assertEqual(len(monthly_avg_volume), 10)
        self.assertEqual(len(monthly_average_stock_price), 10)
        self.assertEqual(len(monthly_average_stock_volume), 10)


if __name__ == '__main__':
    unittest.main()

    
    
    
