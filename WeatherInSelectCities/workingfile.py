"""
This program generates and stores daily temperature from 2010 to 2023 for a given city.
"""

import json
import sys

import requests

END_POINT = "http://api.weatherapi.com/v1/history.json"
API_TOKEN = "29ee4eb00b764085a5315938232303"
RECORD_HOUR = "12"
UNIT_TYPE = "metric"
PATH_NAME = "lynnwood_historical_temperature.json"

# read dates from interval_dates.text
# file1 = open("interval_dates.txt", "r")
# Lines = file1.readlines()
dates = ['2023-01-01', '2023-01-10']
date_start = f"\"{dates[0]}\""
date_end = f"\"{dates[1]}\""
print(date_start, date_end)
params = {
    "q": "47.8278656,-122.3053932",
    "key": API_TOKEN,
    "dt": dates[0],
    "end_dt": dates[1],
    "hour": RECORD_HOUR,
    "alerts": "no",
}
response = requests.get(END_POINT, params=params)
print(response)
if response.status_code != 200:
    print(f"Error {response.status_code} while retrieving data from {END_POINT}")
    sys.exit(1)

data = response.json()
print(data)
#
# for line in Lines:
#     date_start = str(line[:10].rstrip())
#     date_end = str(line[12:].rstrip().lstrip())



    # with open(PATH_NAME, "w") as f:
    #     json.dump(data, f)
    # print(json.dumps(data, indent=4))

#
# PATH_NAME.close()
