import os
import sys
import unittest

sys.path.append(os.path.abspath("src"))

from src.core.elite_status import Elite_Status
from src.tests.test_elite_status import TestEliteStatus


def run_tests():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestEliteStatus)
    unittest.TextTestRunner(verbosity=2).run(suite)


def main():
    # Run tests first
    run_tests()

    # If tests pass, run the main program
    elite_status = Elite_Status()
    elite_status.massage_dataframes()
    elite_status.calculate_elite_membership()
    elite_status.avg_flights()


if __name__ == "__main__":
    main()
