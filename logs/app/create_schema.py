import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Default database connection settings
DEFAULT_DB_USER = "postgres"
DEFAULT_DB_PASSWORD = "your_password"
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = "5432"
DEFAULT_DB_NAME = "api_analytics"


def get_db_connection_params(database="postgres"):
    """Get database connection parameters from environment variables or defaults"""
    return {
        "dbname": database,
        "user": os.getenv("POSTGRES_USER", DEFAULT_DB_USER),
        "password": os.getenv("POSTGRES_PASSWORD", DEFAULT_DB_PASSWORD),
        "host": os.getenv("POSTGRES_HOST", DEFAULT_DB_HOST),
        "port": os.getenv("POSTGRES_PORT", DEFAULT_DB_PORT),
    }


def create_database_and_schema():
    # Connect to PostgreSQL server to create database
    conn = psycopg2.connect(**get_db_connection_params())
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    db_name = os.getenv("POSTGRES_DB", DEFAULT_DB_NAME)

    # Create database if it doesn't exist
    cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}'")
    if not cursor.fetchone():
        cursor.execute(f"CREATE DATABASE {db_name}")

    cursor.close()
    conn.close()

    # Connect to the newly created database
    conn = psycopg2.connect(**get_db_connection_params(db_name))
    cursor = conn.cursor()

    # Create tables with appropriate indexes
    create_tables = """
    -- Create customer_daily_stats table
    CREATE TABLE IF NOT EXISTS customer_daily_stats (
        id SERIAL PRIMARY KEY,
        customer_id INTEGER NOT NULL,
        date DATE NOT NULL,
        successful_requests INTEGER DEFAULT 0,
        failed_requests INTEGER DEFAULT 0,
        uptime_seconds INTEGER DEFAULT 0,
        avg_latency FLOAT DEFAULT 0.0,
        median_latency FLOAT DEFAULT 0.0,
        p99_latency FLOAT DEFAULT 0.0,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT unique_customer_date
            UNIQUE(customer_id, date)
    );

    -- Create index for efficient querying by customer_id and date
    CREATE INDEX IF NOT EXISTS idx_customer_stats_customer_date 
    ON customer_daily_stats (customer_id, date);

    -- Create index for date range queries
    CREATE INDEX IF NOT EXISTS idx_customer_stats_date 
    ON customer_daily_stats (date);
    """

    cursor.execute(create_tables)

    # Create function to automatically update updated_at timestamp
    update_timestamp_function = """
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    $$ language 'plpgsql';
    """
    cursor.execute(update_timestamp_function)

    # Create trigger for automatic timestamp updates
    create_trigger = """
    DROP TRIGGER IF EXISTS update_customer_stats_updated_at ON customer_daily_stats;
    CREATE TRIGGER update_customer_stats_updated_at
        BEFORE UPDATE ON customer_daily_stats
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """
    cursor.execute(create_trigger)

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    create_database_and_schema()
