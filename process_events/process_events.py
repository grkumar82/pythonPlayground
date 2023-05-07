"""
This program process events.csv and outputs number of events by customer_id within one hour buckets.
Results of this program should be available here: http://localhost:8080/processed_events.json
"""
import csv
import json
import logging
import re
import unittest
from collections import defaultdict

from dateutil import parser

# excluding all events with this event_type
PROXY_EVENT = "INGEST-WITH-PROXY"

# files for input and outputting
INPUT_FILE = "events.csv"
OUTPUT_FILE = "processed_events.json"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProcessEvents:
    def __init__(self):
        self.input_file = INPUT_FILE
        self.output_file = OUTPUT_FILE
        # key is customer_id; value is a dict of timestamp as key and number of processed events within the past hour
        self.customer_events = defaultdict(dict)
        self.skipped_rows = 0
        self.output = {}

    @staticmethod
    def process_customer_id(id):
        pattern = r"^[a-f0-9]{32}$"
        match = re.match(pattern, id)
        return match.group(0) if match else None

    @staticmethod
    def process_timestamps(ts):
        timestamp = parser.parse(ts)
        # round up to nearest hour
        timestamp_rounded = timestamp.replace(minute=0, second=0, microsecond=0)
        # format the timestamp as a string in the desired format
        return timestamp_rounded.strftime("%Y-%m-%d %H:%M:%S")

    def process_file(self):
        with open(self.input_file, newline="") as csvfile:
            reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            rows = (row for row in reader)

            for row in rows:
                row_length = len(row)
                if row_length == 4:
                    if len(row[1]) != 6 or row[1] == PROXY_EVENT or row[2] == "%test%":
                        self.skipped_rows += 1
                        continue
                    processed_customer_id = self.process_customer_id(row[0])
                    if not processed_customer_id:
                        self.skipped_rows += 1
                        continue
                    processed_timestamp = self.process_timestamps(row[3])
                    self.customer_events[processed_customer_id][processed_timestamp] = (
                        self.customer_events[processed_customer_id].get(
                            processed_timestamp, 0
                        )
                        + 1
                    )
            if self.skipped_rows:
                logger.warning(
                    f"{self.skipped_rows} row(s) are not processed due to format not matching to requirements."
                )
            print("\n")

        with open(self.output_file, "w") as f:
            json.dump(self.customer_events, f)

    def output_valid_buckets(self, customer_id, start_ts, end_ts, output):
        buckets = self.customer_events[customer_id]
        for time_stamp, values in buckets.items():
            if start_ts <= time_stamp <= end_ts and time_stamp not in output:
                output[time_stamp] = values
        return output

    def ts_format_check(self, ts):
        pattern_ts = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
        match = re.match(pattern_ts, ts)
        if match:
            return True
        else:
            return False

    def output_values_for_user(self):
        print("\n")
        request_prompt = input(
            "Do you want to input customer_id and time_stamp? Input 'Y' for yes or 'N' to exit the "
            "program. Any other input will cause the program to exit. \n"
        )
        if request_prompt == "N":
            print("Exiting program! \n")
            exit()
        elif request_prompt != "Y":
            print("Invalid input, exiting program! \n")
            exit()
        else:
            customer_id_to_check = input("Enter customer_id: \n")
            processed_customer_id = self.process_customer_id(customer_id_to_check)
            if not processed_customer_id:
                print("Invalid input, exiting program! \n")
                exit()
            if customer_id_to_check not in self.customer_events:
                print("This customer_id is not in the log hence exiting program! \n")
                exit()
            print(
                "Enter timestamps of format type - YYYY-MM-DD HH:MM:SS, example - 2021-03-01 13:00:00.\n"
            )
            ts_start = input("Enter start timestamp: \n")
            ts_end = input("Enter end timestamp: \n")
            if not self.ts_format_check(ts_start) or not self.ts_format_check(ts_end):
                print("Invalid input, exiting program! \n")
                exit()
        ts_start, ts_end = self.process_timestamps(ts_start), self.process_timestamps(
            ts_end
        )
        self.output_valid_buckets(customer_id_to_check, ts_start, ts_end, self.output)

    def output_values(self):
        if not self.output:
            print("No values in the provided timestamps! \n")
        else:
            print("These are the values for the timestamps provided! \n")
            for key, values in self.output.items():
                print(key, values)


class TestProcessEvents(unittest.TestCase):
    def test_process_customer_id(self):
        # test whether appropriately customer_id is processed or not
        pe = ProcessEvents()
        self.assertEqual(
            pe.process_customer_id("56789012345678901212345678901234"),
            "56789012345678901212345678901234",
        )
        self.assertEqual(pe.process_customer_id("123456"), None)
        self.assertEqual(
            pe.process_customer_id("789012345612345678901234567890123"), None
        )

    def test_process_timestamps(self):
        # test whether timestamps are outputting based on provided requirements.
        pe = ProcessEvents()
        ts1 = str("2021-03-03 19:47:59.868")
        str2 = str("2021-03-03 19:00:00")
        self.assertEqual(
            pe.process_timestamps(ts1),
            str2,
        )
        str3 = str("2021-03-03 20:00:00")
        str4 = str("2021-03-03 17:00:58.1")
        str5 = str("2021-03-03 17:00:00")
        self.assertNotEqual(
            pe.process_timestamps(ts1),
            str3,
        )
        self.assertEqual(
            pe.process_timestamps(str4),
            str5,
        )

    def test_results_accuracy(self):
        pe = ProcessEvents()
        pe.process_file()
        self.assertEqual(
            pe.customer_events["1abb42414607955dbf6088b99f837d8f"][
                "2021-03-01 04:00:00"
            ],
            2,
        )
        self.assertEqual(
            pe.customer_events["1abb42414607955dbf6088b99f837d8f"][
                "2021-03-01 15:00:00"
            ],
            13,
        )
        self.assertNotEqual(
            pe.customer_events["1abb42414607955dbf6088b99f837d8f"][
                "2021-03-01 16:00:00"
            ],
            20,
        )


if __name__ == "__main__":
    unittest.main()

answer = ProcessEvents()
answer.process_file()
answer.output_values_for_user()
answer.output_values()
