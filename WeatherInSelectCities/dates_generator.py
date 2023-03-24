"""
This program generates dates from 01/01/2020 till 03/23/2023 in 29 day intervals.
"""

import datetime
import unittest

START_DATE_2010 = "2010-01-01"
FORMAT = "%Y-%m-%d"
END_DATE_2023_STR = "2023-03-23"


def convertStringToDate(input_str):
    return datetime.datetime.strptime(input_str, FORMAT).date()


END_DATE_2023 = convertStringToDate(END_DATE_2023_STR)


def convertDatesToString(input_date):
    return str(input_date)


def addDays(input_start_date):
    input_start_date = convertDatesToString(input_start_date)
    convert_input_date = datetime.datetime.strptime(input_start_date, FORMAT).date()
    interval_end_date = convert_input_date + datetime.timedelta(days=29)
    return interval_end_date


interval_dates = []

interval_end_date = addDays(START_DATE_2010)
interval_start_date = convertStringToDate(START_DATE_2010)

while interval_end_date < END_DATE_2023:
    interval_end_date_str = convertDatesToString(interval_end_date)
    start_date_str = convertDatesToString(interval_start_date)
    interval_dates.append([start_date_str, interval_end_date_str])
    interval_start_date = interval_end_date + datetime.timedelta(days=1)
    interval_end_date = addDays(interval_start_date)

dates_file = open("interval_dates.txt", "w")

for items in interval_dates:
    start_date, end_date = items
    dates_file.write(start_date + "  " + end_date + "\n")

dates_file.close()


class TestDatesGenerator(unittest.TestCase):
    def test_list_len(self):
        self.assertGreater(len(interval_dates), 0)

    def test_start_end_years(self):
        self.assertEqual(interval_dates[0][0][:4], "2010")
        self.assertEqual(interval_dates[-1][0][:4], "2023")


if __name__ == "__main__":
    unittest.main()
