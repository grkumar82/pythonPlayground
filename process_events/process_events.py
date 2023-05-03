"""
This program process events.csv and outputs number of events by customer_id within one hour buckets.
Results of this program should be available here: http://localhost:8080/processed_events.json
"""
import csv
import http.server
import json
import logging
import re
import socketserver
import threading
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
        self.port = 8080
        self.server_thread = None

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

        with open(self.output_file, "w") as f:
            json.dump(self.customer_events, f)

    def http_output(self):
        handler = http.server.SimpleHTTPRequestHandler
        with socketserver.TCPServer(("", self.port), handler) as httpd:
            logger.info(f"Serving at port {self.port}")
            self.server_thread = threading.Thread(target=httpd.serve_forever)
            self.server_thread.start()

            # Add a timer to shut down the server after 10 minutes
            timer = threading.Timer(600, self.shutdown_server, args=[httpd])
            timer.start()

            # Wait for the server thread to complete
            self.server_thread.join()

    @staticmethod
    def shutdown_server(httpd):
        httpd.shutdown()


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
         # test accuracy of couple of data points to ensure program does as it's intended.
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
answer.http_output()
