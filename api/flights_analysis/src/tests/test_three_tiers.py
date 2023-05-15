import unittest

from src.core.three_tiers import Three_Tiers


class TestThreeTiers(unittest.TestCase):
    def test_three_tiers(self):
        three_tiers = Three_Tiers()
        three_tiers_list = three_tiers.three_tiers()
        self.assertEqual(len(three_tiers_list), 2)


if __name__ == "__main__":
    unittest.main()
