from __future__ import annotations

import argparse
import logging
import sys

import awswrangler as wr


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

BRONZE_DATABASE = "flights_bronze"
SILVER_DATABASE = "flights_silver"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    return parser.parse_args()


def run_query(sql: str):
    wr.athena.start_query_execution(
        sql=sql,
        database=BRONZE_DATABASE,
        wait=True,
    )


def main():
    args = parse_args()

    logger.info("Creating database %s", SILVER_DATABASE)
    wr.catalog.create_database(name=SILVER_DATABASE, exist_ok=True)

    
    logger.info("Creating flights_daily")

    run_query(f"""
    DROP TABLE IF EXISTS {SILVER_DATABASE}.flights_daily
    """)

    run_query(f"""
    CREATE TABLE {SILVER_DATABASE}.flights_daily
    WITH (
        format='PARQUET',
        write_compression='SNAPPY',
        external_location='s3://{args.bucket}/flights/silver/flights_daily/',
        partitioned_by=ARRAY['month']
    ) AS
    SELECT
    year,
    day,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN departure_delay > 0 THEN 1 ELSE 0 END) AS total_delayed,
    SUM(CAST(cancelled AS INTEGER)) AS total_cancelled,
    AVG(CASE WHEN cancelled = 0 THEN departure_delay END) AS avg_departure_delay,
    AVG(CASE WHEN cancelled = 0 THEN arrival_delay END) AS avg_arrival_delay,
    month
FROM flights_bronze.flights
GROUP BY year, month, day
    """)

    
    logger.info("Creating flights_monthly")

    run_query(f"""
    DROP TABLE IF EXISTS {SILVER_DATABASE}.flights_monthly
    """)

    run_query(f"""
    CREATE TABLE {SILVER_DATABASE}.flights_monthly
    WITH (
        format='PARQUET',
        write_compression='SNAPPY',
        external_location='s3://{args.bucket}/flights/silver/flights_monthly/'
    ) AS
    SELECT
        month,
        airline,
        COUNT(*) AS total_flights,
        SUM(CASE WHEN departure_delay > 0 THEN 1 ELSE 0 END) AS total_delayed,
        SUM(CAST(cancelled AS INTEGER)) AS total_cancelled,
        AVG(CASE WHEN cancelled = 0 THEN arrival_delay END) AS avg_arrival_delay,
        100.0 * AVG(
            CASE
                WHEN cancelled = 0 AND arrival_delay <= 15 THEN 1
                ELSE 0
            END
        ) AS on_time_pct
    FROM {BRONZE_DATABASE}.flights
    GROUP BY month, airline
    """)

    
    logger.info("Creating flights_by_airport")

    run_query(f"""
    DROP TABLE IF EXISTS {SILVER_DATABASE}.flights_by_airport
    """)

    run_query(f"""
    CREATE TABLE {SILVER_DATABASE}.flights_by_airport
    WITH (
        format='PARQUET',
        write_compression='SNAPPY',
        external_location='s3://{args.bucket}/flights/silver/flights_by_airport/'
    ) AS
    SELECT
        origin_airport,
        COUNT(*) AS total_departures,
        SUM(CASE WHEN departure_delay > 0 THEN 1 ELSE 0 END) AS total_delayed,
        SUM(CAST(cancelled AS INTEGER)) AS total_cancelled,
        AVG(CASE WHEN cancelled = 0 THEN departure_delay END) AS avg_departure_delay,
        CASE
            WHEN SUM(CASE WHEN departure_delay > 0 THEN departure_delay ELSE 0 END) > 0
            THEN 100.0 * SUM(COALESCE(weather_delay, 0))
                 / SUM(CASE WHEN departure_delay > 0 THEN departure_delay ELSE 0 END)
            ELSE 0
        END AS pct_weather_delay
    FROM {BRONZE_DATABASE}.flights
    GROUP BY origin_airport
    """)

    logger.info("Silver layer completed successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Silver failed")
        sys.exit(1)