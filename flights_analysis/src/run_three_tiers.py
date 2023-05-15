import os
import sys
import unittest

sys.path.append(os.path.abspath("src"))

from src.core.three_tiers import Three_Tiers
from src.tests.test_three_tiers import TestThreeTiers


def run_tests():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestThreeTiers)
    unittest.TextTestRunner(verbosity=2).run(suite)


def main():
    # Run tests first
    run_tests()

    # If tests pass, run the main program
    three_tiers = Three_Tiers()
    three_tiers.three_tiers()
    three_tiers.output_graph()


if __name__ == "__main__":
    main()
