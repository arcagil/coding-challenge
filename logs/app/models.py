from datetime import date
from sqlalchemy import Column, Integer, String, Date, Float, create_engine
from sqlalchemy.ext.declarative import declarative_base
import os

Base = declarative_base()


class CustomerDailyStats(Base):
    __tablename__ = "customerdailystats"

    id = Column(Integer, primary_key=True)
    customer_id = Column(String, index=True)
    date = Column(Date, index=True)
    successful_requests = Column(Integer)
    failed_requests = Column(Integer)
    uptime_percentage = Column(Float)
    avg_latency = Column(Float)
    median_latency = Column(Float)
    p99_latency = Column(Float)


# Database connection
database_url = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/logstats"
)
engine = create_engine(database_url)


# Create tables
def create_db_and_tables():
    Base.metadata.create_all(engine)
