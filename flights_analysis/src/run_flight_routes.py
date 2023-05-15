import os
import sys
import unittest

sys.path.append(os.path.abspath("src"))

from src.core.flight_routes import Flight_Routes
from src.tests.test_flight_routes import TestFlightRoutes


def run_tests():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestFlightRoutes)
    unittest.TextTestRunner(verbosity=2).run(suite)


def main():
    # Run tests first
    run_tests()

    # If tests pass, run the main program
    flight_routes = Flight_Routes()
    flight_routes.identify_daily_duplicate_flights()
    flight_routes.identify_weekly_duplicate_flights()
    flight_routes.write_to_csv()


if __name__ == "__main__":
    main()
