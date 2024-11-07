import os
import unittest
from datetime import datetime
from unittest.mock import patch, MagicMock
import tempfile
import numpy as np
from bytewax.testing import TestingSink, run_main
import bytewax.operators as op

from log_processor import (
    LogEntry,
    DailyStats,
    parse_log_line,
    build_dataflow,
    save_to_database,
)

# Sample log entries for testing
SAMPLE_LOGS = [
    "2024-10-26 03:05:00 cust_1 /api/v1/resource2 200 0.5",
    "2024-10-26 03:06:00 cust_1 /api/v1/resource2 200 0.75",
    "2024-10-26 03:07:00 cust_1 /api/v1/resource1 403 1.",
    "2024-10-27 03:07:00 cust_1 /api/v1/resource1 200 2.",
    "2024-10-26 03:08:00 cust_2 /api/v1/resource1 200 1.",
    "2024-10-26 03:09:00 cust_2 /api/v1/resource1 500 1.",  # Start of downtime
    "2024-10-26 03:09:01 cust_2 /api/v1/resource1 503 1.",  # Still down
    "2024-10-26 03:09:02 cust_2 /api/v1/resource1 200 1.",  # Recovery
    "2024-10-28 03:09:08 cust_20 /api/v1/resource1 200 3.",
]


class TestLogProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures"""
        # Create temporary log file
        self.temp_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
        for log in SAMPLE_LOGS:
            self.temp_file.write(log + "\n")
        self.temp_file.close()

        # Mock database session
        self.patcher = patch("log_processor.Session")
        self.mock_session = self.patcher.start()
        self.session_instance = MagicMock()
        self.mock_session.return_value.__enter__.return_value = self.session_instance

    def tearDown(self):
        """Clean up test fixtures"""
        os.unlink(self.temp_file.name)
        self.patcher.stop()

    def test_dataflow_processing(self):
        """Test the complete dataflow processing"""
        results = []

        # Build dataflow and get stream
        flow, stream = build_dataflow(self.temp_file.name)

        # Add output step
        test_sink = TestingSink(results)
        op.output("output", stream, test_sink)

        # Run the dataflow
        run_main(flow)

        # Verify basic results structure
        self.assertEqual(
            len(results), 4
        )  # Should be 4 customers/dates: cust_1/26, cust_1/27, cust_2/26, cust_20/28

        # Check customer 1 stats for Oct 26
        cust_1_stats_26 = next(
            r
            for r in results
            if r["customer_id"] == "cust_1" and r["date"] == "2024-10-26"
        )
        self.assertEqual(cust_1_stats_26["successful_requests"], 2)
        self.assertEqual(cust_1_stats_26["failed_requests"], 1)
        self.assertEqual(cust_1_stats_26["uptime_percentage"], 100.0)
        self.assertAlmostEqual(cust_1_stats_26["avg_latency"], 0.75)
        self.assertAlmostEqual(cust_1_stats_26["median_latency"], 0.75)
        self.assertAlmostEqual(cust_1_stats_26["p99_latency"], 0.995)

        # Check customer 1 stats for Oct 27
        cust_1_stats_27 = next(
            r
            for r in results
            if r["customer_id"] == "cust_1" and r["date"] == "2024-10-27"
        )
        self.assertEqual(cust_1_stats_27["successful_requests"], 1)
        self.assertEqual(cust_1_stats_27["failed_requests"], 0)
        self.assertEqual(cust_1_stats_27["uptime_percentage"], 100.0)
        self.assertAlmostEqual(cust_1_stats_27["avg_latency"], 2.0)
        self.assertAlmostEqual(cust_1_stats_27["median_latency"], 2.0)
        self.assertAlmostEqual(cust_1_stats_27["p99_latency"], 2.0)

        # Check customer 2 stats
        cust_2_stats = next(r for r in results if r["customer_id"] == "cust_2")
        self.assertEqual(cust_2_stats["date"], "2024-10-26")
        self.assertEqual(cust_2_stats["successful_requests"], 2)
        self.assertEqual(cust_2_stats["failed_requests"], 2)
        seconds_in_day = 24 * 60 * 60
        downtime_seconds = 2  # 2 seconds of downtime based on log entries
        expected_uptime = ((seconds_in_day - downtime_seconds) / seconds_in_day) * 100
        self.assertAlmostEqual(cust_2_stats["uptime_percentage"], expected_uptime)
        self.assertAlmostEqual(cust_2_stats["avg_latency"], 1.0)
        self.assertAlmostEqual(cust_2_stats["median_latency"], 1.0)
        self.assertAlmostEqual(cust_2_stats["p99_latency"], 1.0)

        # Check customer 20 stats
        cust_20_stats = next(r for r in results if r["customer_id"] == "cust_20")
        self.assertEqual(cust_20_stats["date"], "2024-10-28")
        self.assertEqual(cust_20_stats["successful_requests"], 1)
        self.assertEqual(cust_20_stats["failed_requests"], 0)
        self.assertEqual(cust_20_stats["uptime_percentage"], 100.0)
        self.assertAlmostEqual(cust_20_stats["avg_latency"], 3.0)
        self.assertAlmostEqual(cust_20_stats["median_latency"], 3.0)
        self.assertAlmostEqual(cust_20_stats["p99_latency"], 3.0)

        # Verify proper grouping
        customer_ids = {r["customer_id"] for r in results}
        self.assertEqual(customer_ids, {"cust_1", "cust_2", "cust_20"})

        date_groups = {(r["customer_id"], r["date"]) for r in results}
        expected_groups = {
            ("cust_1", "2024-10-26"),
            ("cust_1", "2024-10-27"),
            ("cust_2", "2024-10-26"),
            ("cust_20", "2024-10-28"),
        }
        self.assertEqual(date_groups, expected_groups)

    def test_parse_log_line(self):
        """Test log line parsing"""
        # Test valid log line
        line = SAMPLE_LOGS[0]
        entry = parse_log_line(line)

        self.assertIsNotNone(entry)
        self.assertIsInstance(entry, LogEntry)
        self.assertEqual(entry.customer_id, "cust_1")
        self.assertEqual(entry.status_code, 200)
        self.assertEqual(entry.duration, 0.5)
        self.assertEqual(entry.request_path, "/api/v1/resource2")

        # Test invalid log line
        invalid_line = "invalid log format"
        self.assertIsNone(parse_log_line(invalid_line))

    def test_daily_stats_calculations(self):
        """Test DailyStats calculations"""
        stats = DailyStats("cust_1", datetime.now())

        # Add successful request
        stats.successful_requests += 1
        stats.latencies.append(0.500)

        # Add failed request
        stats.failed_requests += 1
        stats.latencies.append(1.500)
        stats.latencies.append(1.600)  # Add one more latency for better p99 calculation

        # Test calculations
        self.assertEqual(stats.successful_requests, 1)
        self.assertEqual(stats.failed_requests, 1)
        self.assertEqual(stats.avg_latency, np.mean([0.5, 1.5, 1.6]))
        self.assertEqual(stats.median_latency, np.median([0.5, 1.5, 1.6]))
        self.assertGreaterEqual(stats.p99_latency, 1.5)

    def test_save_to_database(self):
        """Test database save operation with mocked session"""
        stats_dict = {
            "customer_id": "cust_1",
            "date": "2024-10-26",
            "successful_requests": 2,
            "failed_requests": 1,
            "uptime_percentage": 1,
            "avg_latency": 0.75,
            "median_latency": 0.75,
            "p99_latency": 0.995,
        }

        # Mock the query filter
        self.session_instance.query.return_value.filter.return_value.first.return_value = (
            None
        )

        # Call the function
        save_to_database(stats_dict)

        # Verify the session was used correctly
        self.session_instance.add.assert_called_once()
        self.session_instance.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
