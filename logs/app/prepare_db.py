from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple
from statistics import mean, median
import numpy as np
import socket
import time
import os

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.operators.windowing import TumblingWindower, EventClock
from bytewax.connectors.files import FileSource
from bytewax.operators import windowing as win
from sqlalchemy.orm import Session
from bytewax.testing import run_main
import alembic.config

from models import CustomerDailyStats, engine, create_db_and_tables


def wait_for_database():
    """Wait for the database to be ready"""
    print("Waiting for database...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    while True:
        try:
            s.connect(("db", 5432))
            s.close()
            print("Database is ready!")
            break
        except socket.error:
            time.sleep(1)


def run_migrations():
    """Run database migrations"""
    print("Running database migrations...")
    alembic_cfg = alembic.config.Config("alembic.ini")
    alembic.command.upgrade(alembic_cfg, "head")
    print("Migrations completed")


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


if __name__ == "__main__":
    wait_for_database()
    run_migrations()
    verify_data()
