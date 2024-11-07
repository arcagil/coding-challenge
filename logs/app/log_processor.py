import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple
from statistics import mean, median

import numpy as np
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.operators.windowing import TumblingWindower, EventClock
from bytewax.connectors.files import FileSource
from bytewax.operators import windowing as win
from sqlalchemy.orm import Session
from bytewax.testing import run_main

from models import CustomerDailyStats, engine


@dataclass
class LogEntry:
    """Represents a single log entry."""

    timestamp: datetime
    customer_id: str
    request_path: str
    status_code: int
    duration: float


@dataclass
class DailyStats:
    """Represents aggregated daily statistics for a customer."""

    customer_id: str
    date: datetime
    successful_requests: int = 0
    failed_requests: int = 0
    uptime_percentage: float = 100.0
    latencies: List[float] = None

    def __post_init__(self):
        if self.latencies is None:
            self.latencies = []

    @property
    def avg_latency(self) -> float:
        return mean(self.latencies) if self.latencies else 0.0

    @property
    def median_latency(self) -> float:
        return median(self.latencies) if self.latencies else 0.0

    @property
    def p99_latency(self) -> float:
        return np.percentile(self.latencies, 99) if self.latencies else 0.0

    def to_db_model(self) -> CustomerDailyStats:
        """Convert to database model"""
        return CustomerDailyStats(
            customer_id=self.customer_id,
            date=self.date.date().replace(tzinfo=timezone.utc),
            successful_requests=self.successful_requests,
            failed_requests=self.failed_requests,
            uptime_percentage=self.uptime_percentage,
            avg_latency=self.avg_latency,
            median_latency=self.median_latency,
            p99_latency=self.p99_latency,
        )


def build_dataflow(input_path: str) -> Tuple[Dataflow, object]:
    """Build the dataflow for processing logs."""
    flow = Dataflow("log_processor")

    source = FileSource(input_path)
    stream = op.input("input", flow, source)

    parsed = op.map("parse", stream, parse_log_line)
    valid_entries = op.filter("valid", parsed, lambda x: x is not None)

    def customer_date_event(entry: LogEntry) -> Tuple[str, LogEntry]:
        return f"{entry.customer_id}_{entry.timestamp.date()}", entry

    keyed = op.map("customer_date_event", valid_entries, customer_date_event)

    event_time_config = EventClock(
        ts_getter=lambda e: e.timestamp,
        wait_for_system_duration=timedelta(days=1),
        now_getter=lambda: datetime.now().replace(tzinfo=timezone.utc),
    )
    clock_config = TumblingWindower(
        length=timedelta(days=1),
        align_to=datetime(2024, 11, 7).replace(tzinfo=timezone.utc),
    )
    window = win.collect_window(
        "windowed_data", keyed, clock=event_time_config, windower=clock_config
    )

    def format_stats(key_stats):
        key, stats = key_stats
        offset, entries = stats

        daily_stats = DailyStats(
            customer_id=entries[0].customer_id if entries else "",
            date=entries[0].timestamp if entries else datetime.now(timezone.utc),
        )

        daily_stats.uptime_percentage = calculate_uptime_percentage(entries)

        for entry in entries:
            if 200 <= entry.status_code < 300:
                daily_stats.successful_requests += 1
            else:
                daily_stats.failed_requests += 1
            daily_stats.latencies.append(entry.duration)

        return {
            "customer_id": daily_stats.customer_id,
            "date": daily_stats.date.strftime("%Y-%m-%d"),
            "successful_requests": daily_stats.successful_requests,
            "failed_requests": daily_stats.failed_requests,
            "uptime_percentage": daily_stats.uptime_percentage,
            "avg_latency": daily_stats.avg_latency,
            "median_latency": daily_stats.median_latency,
            "p99_latency": daily_stats.p99_latency,
        }

    formatted = op.map("format_stats", window.down, format_stats)
    return flow, formatted


def parse_log_line(line: str) -> Optional[LogEntry]:
    """Parse a single log line into a LogEntry."""
    try:
        parts = line.strip().split()
        timestamp = datetime.strptime(
            f"{parts[0]} {parts[1]}", "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=timezone.utc)
        return LogEntry(
            timestamp=timestamp,
            customer_id=parts[2],
            request_path=parts[3],
            status_code=int(parts[4]),
            duration=float(parts[5]),
        )
    except (IndexError, ValueError) as e:
        print(f"Error parsing line: {line}. Error: {e}")
        return None


def save_to_database(stats_dict: dict):
    """Save the statistics to the database"""
    with Session(engine) as session:
        db_stats = CustomerDailyStats(
            customer_id=stats_dict["customer_id"],
            date=datetime.strptime(stats_dict["date"], "%Y-%m-%d").date(),
            successful_requests=stats_dict["successful_requests"],
            failed_requests=stats_dict["failed_requests"],
            uptime_percentage=stats_dict["uptime_percentage"],
            avg_latency=stats_dict["avg_latency"],
            median_latency=stats_dict["median_latency"],
            p99_latency=stats_dict["p99_latency"],
        )

        existing = (
            session.query(CustomerDailyStats)
            .filter(
                CustomerDailyStats.customer_id == db_stats.customer_id,
                CustomerDailyStats.date == db_stats.date,
            )
            .first()
        )

        if existing:
            for key, value in stats_dict.items():
                if key not in ["customer_id", "date"]:
                    setattr(existing, key, value)
            session.add(existing)
        else:
            session.add(db_stats)

        session.commit()


def save_to_db_step(stream):
    """Add database save step to the dataflow"""
    return op.map("save_to_db", stream, save_to_database)


def calculate_uptime_percentage(entries: List[LogEntry]) -> float:
    """Calculate uptime percentage for a given day's entries."""
    if not entries:
        return 100.0

    total_duration = 24 * 60 * 60  # Total seconds in a day
    downtime = 0
    down_start = None

    for entry in entries:
        is_error = 500 <= entry.status_code < 600
        if is_error and down_start is None:
            down_start = entry.timestamp
        elif not is_error and down_start is not None:
            downtime += (entry.timestamp - down_start).total_seconds()
            down_start = None

    if down_start is not None:
        downtime += (entries[-1].timestamp - down_start).total_seconds()

    uptime_percentage = ((total_duration - downtime) / total_duration) * 100
    return max(0.0, min(100.0, uptime_percentage))


def verify_data():
    """Verify data in database"""
    print("Verifying data in database...")
    with Session(engine) as session:
        count = session.query(CustomerDailyStats).count()
        print(f"Found {count} records in database")

        if count > 0:
            records = session.query(CustomerDailyStats).all()
            for record in records:
                print(
                    f"Customer: {record.customer_id}, Date: {record.date}, Uptime: {record.uptime_percentage}%"
                )


def process_logs(log_file: str):
    """Process logs and save to database"""
    print(f"Processing logs from {log_file}...")

    # Build the dataflow
    flow, stream = build_dataflow(log_file)

    # Add save to database step
    save_stream = save_to_db_step(stream)

    # required by bytewax to have at least one output step
    _ = op.inspect("inspect", save_stream)

    # Run the dataflow
    run_main(flow)

    print("Log processing completed")


def main():
    """Main function to run the complete pipeline"""
    # Process logs
    log_file = "api_requests.log"
    if os.path.exists(log_file):
        process_logs(log_file)
        verify_data()
    else:
        print(f"Log file not found: {log_file}")


if __name__ == "__main__":
    main()
