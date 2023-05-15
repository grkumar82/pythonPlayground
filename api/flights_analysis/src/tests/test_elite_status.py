import unittest

from src.core.elite_status import Elite_Status


class TestEliteStatus(unittest.TestCase):
    def test_calculate_elite_membership(self):
        elite_status = Elite_Status()
        membership = elite_status.calculate_elite_membership()
        self.assertGreater(membership, 0)

    def test_avg_flights(self):
        elite_status = Elite_Status()
        result = elite_status.avg_flights()
        self.assertGreater(result, 0)


if __name__ == "__main__":
    unittest.main()
