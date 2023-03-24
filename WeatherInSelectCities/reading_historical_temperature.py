"""
This program generates and stores daily temperature from 2010 to 2023 for a given city.
"""

import json
import sys

import requests

END_POINT = "http://api.weatherapi.com/v1/history.json"
API_TOKEN = "29ee4eb00b764085a5315938232303"
RECORD_HOUR = "9"
UNIT_TYPE = "metric"
PATH_NAME = "lynnwood_historical_temperature.json"

# read dates from interval_dates.text
file1 = open("interval_dates.txt", "r")
Lines = file1.readlines()

for line in Lines:
    date_start = str(line[:10].rstrip())
    date_end = str(line[12:].rstrip().lstrip())
    params = {
        "key": API_TOKEN,
        "q": "47.8278656,-122.3053932",
        "dt": date_start,
        "end_dt": date_end,
        "hour": RECORD_HOUR,
        "alerts": "no",
    }

    response = requests.get(END_POINT, params=params)
    print(response)
    if response.status_code != 200:
        print(f"Error {response.status_code} while retrieving data from {END_POINT}")
        sys.exit(1)

    data = response.json()
    with open(PATH_NAME, "w") as f:
        json.dump(data, f)
    print(json.dumps(data, indent=4))


PATH_NAME.close()
