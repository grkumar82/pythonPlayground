import unittest

from lib.utils import format_amount, parse_data_json


class UtilsTestCase(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super().__init__(methodName)
        self.testdata = {
            "id": 291,
            "userId": 3,
            "userName": "Helena Fernandez",
            "timestamp": 1566827268242,
            "txnType": "debit",
            "amount": "$1,342.02",
            "location": {
                "id": 9,
                "address": "961, Neptide, Elliott Walk",
                "city": "Bourg",
                "zipCode": 68602,
            },
            "ip": "132.169.40.222",
        }

        self.amount = "$1,012.23343"

    def test_number_conversion(self):
        formatted_test_amt = format_amount(self.amount)
        self.assertEqual(formatted_test_amt, 1012.23343)

    def test_parsed_data(self):
        userId, userName, timestamp, txnType, _amt = parse_data_json(self.testdata)
        self.assertEqual(userId, 3)
        self.assertEqual(userName, "Helena Fernandez")
        self.assertEqual(timestamp, 1566827268242)
        self.assertEqual(txnType, "debit")
        self.assertEqual(_amt, 1342.02)


if __name__ == "__main__":
    unittest.main()