import sys
from collections import *

import requests

from lib.utils import *

END_POINT = "https://jsonmock.hackerrank.com/api/transactions/"


class ProcessJSONData:
    def __init__(self) -> None:
        self.user_mapping, self.final_amount = {}, {}
        self.users_txn = defaultdict(OrderedDict)
        self.TOTAL_PAGES = 0

    def get_pages_count(self):
        response = requests.get(END_POINT)

        if response.status_code != 200:
            print(
                f"Error {response.status_code} while retrieving data from {END_POINT}"
            )
            sys.exit(-1)

        data = response.json()
        self.TOTAL_PAGES = data["total_pages"]

    def process_json_data(self):
        for num in range(self.TOTAL_PAGES + 1):
            page_url = END_POINT + "search?page=" + str(num)

            response = requests.get(page_url)

            if response.status_code != 200:
                print(
                    f"Error {response.status_code} while retrieving data from {END_POINT}"
                )
                sys.exit(-1)

            json_data = response.json()
            txn_data = json_data["data"]
            for i in range(len(txn_data)):
                userId, userName, timestamp, txnType, formatted_amt = parse_data_json(
                    txn_data[i]
                )
                self.user_mapping[userId] = userName

                if txnType.lower() == "debit":
                    self.users_txn[userId][timestamp] = -formatted_amt
                elif txnType.lower() == "credit":
                    self.users_txn[userId][timestamp] = formatted_amt

    def process_transactions(self):
        self.final_amount = {}
        for userId, txns in self.users_txn.items():
            amt = 0
            for k, v in txns.items():
                amt += v
            self.final_amount[self.user_mapping[userId]] = round(amt, 2)


if __name__ == "__main__":
    processdata = ProcessJSONData()
    processdata.get_pages_count()
    processdata.process_json_data()
    processdata.process_transactions()
    print(processdata.final_amount)
