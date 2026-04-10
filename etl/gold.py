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

GOLD_DATABASE = "flights_gold"
GOLD_TABLE = "vuelos_analitica"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Gold analytical table for flights dataset."
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="S3 bucket for Athena query results.",
    )
    return parser.parse_args()


def create_database() -> None:
    logger.info("Creating Glue database if it does not exist: %s", GOLD_DATABASE)
    wr.catalog.create_database(name=GOLD_DATABASE, exist_ok=True)


def drop_table_if_exists() -> None:
    logger.info("Dropping table if exists: %s.%s", GOLD_DATABASE, GOLD_TABLE)
    wr.catalog.delete_table_if_exists(
        database=GOLD_DATABASE,
        table=GOLD_TABLE,
    )


def build_ctas_sql() -> str:
    return f"""
    CREATE TABLE {GOLD_DATABASE}.{GOLD_TABLE} AS
    SELECT
        f.year,
        f.month,
        f.day,
        f.origin_airport,
        ap_orig.airport AS origin_airport_name,
        ap_orig.city AS origin_city,
        ap_orig.state AS origin_state,
        f.destination_airport,
        ap_dest.airport AS destination_airport_name,
        al.airline AS airline_name,
        f.departure_delay,
        f.arrival_delay,
        f.cancelled,
        f.cancellation_reason,
        f.distance,
        f.air_system_delay,
        f.airline_delay,
        f.weather_delay,
        f.late_aircraft_delay,
        f.security_delay
    FROM flights_bronze.flights f
    LEFT JOIN flights_bronze.airlines al
        ON f.airline = al.iata_code
    LEFT JOIN flights_bronze.airports ap_orig
        ON f.origin_airport = ap_orig.iata_code
    LEFT JOIN flights_bronze.airports ap_dest
        ON f.destination_airport = ap_dest.iata_code
    """


def create_gold_table(bucket: str) -> None:
    sql = build_ctas_sql()

    logger.info("Creating table %s.%s in Athena", GOLD_DATABASE, GOLD_TABLE)
    wr.athena.read_sql_query(
        sql=sql,
        database=GOLD_DATABASE,
        ctas_approach=False,
        s3_output=f"s3://{bucket}/athena-query-results/",
    )
    logger.info("Gold table created successfully.")


def main() -> None:
    args = parse_args()

    create_database()
    drop_table_if_exists()
    create_gold_table(args.bucket)

    logger.info("Gold layer completed successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Gold ETL failed.")
        sys.exit(1)