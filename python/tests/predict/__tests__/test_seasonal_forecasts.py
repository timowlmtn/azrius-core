import unittest
import pandas as pd
import os
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from predict.seasonal_forecasts import (
    generate_seasonal_forecasts,
    find_critical_points,
    find_annotated_critical_points,
)
from dotenv import load_dotenv

load_dotenv()

# Load the environment variables from the .env file

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")


class TestSeasonalForecasts(unittest.TestCase):
    actual = None
    forecast_stats = None
    predicted_mean = None

    @classmethod
    def setUpClass(cls):
        # Create in-memory SQLite database and sample data
        engine = create_engine(
            f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )

        cls.actual, cls.forecast_stats, cls.predicted_mean = (
            generate_seasonal_forecasts(
                engine=engine, lags=[52], forecast_horizon=12, order=(1, 1, 1)
            )
        )

    def test_generate_forecasts_output_shapes(self):
        print(self.actual)
        self.assertIsInstance(self.actual, pd.Series)

        print(self.forecast_stats)

        print(len(self.forecast_stats[52].keys()))

    def test_critical_points_detected(self):

        critical = find_critical_points(self.predicted_mean)

        print(critical)

        self.assertIn(52, critical)
        self.assertIsInstance(critical[52], list)
        self.assertTrue(all(isinstance(dt, pd.Timestamp) for dt in critical[52]))

    def test_annotated_critical_points_structure(self):
        annotated = find_annotated_critical_points(self.predicted_mean[52], keep=2)
        for entry in annotated:
            print(f"{entry}")
            self.assertIn("timestamp", entry)
            self.assertIn("value", entry)
            self.assertIn("type", entry)
            self.assertIn("is_global", entry)


if __name__ == "__main__":
    unittest.main()
