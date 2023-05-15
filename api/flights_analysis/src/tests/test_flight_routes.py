import unittest

from src.core.flight_routes import Flight_Routes


class TestFlightRoutes(unittest.TestCase):
    def test_identify_daily_duplicate_flights(self):
        flight_routes = Flight_Routes()
        daily_flight_duplicates = flight_routes.identify_daily_duplicate_flights()
        self.assertEqual(len(daily_flight_duplicates), 1)

    def test_identify_weekly_duplicate_flights(self):
        flight_routes = Flight_Routes()
        weekly_flight_duplicates = flight_routes.identify_weekly_duplicate_flights()
        self.assertEqual(len(weekly_flight_duplicates), 2)


if __name__ == "__main__":
    unittest.main()
