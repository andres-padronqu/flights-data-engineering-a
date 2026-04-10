from __future__ import annotations

import json
import logging
from pathlib import Path

import boto3
import pandas as pd
from sqlalchemy import create_engine, insert, text
from sqlalchemy.orm import Session

from db.models import Airline, Airport, Base, Flight


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

DATA_DIR = Path("data/flights")
SECRET_ID = "itam/rds/northwind/credentials"
REGION = "us-east-1"
NROWS_FLIGHTS = 500_000


def get_db_credentials() -> dict[str, str]:
    client = boto3.client("secretsmanager", region_name=REGION)
    secret = client.get_secret_value(SecretId=SECRET_ID)
    return json.loads(secret["SecretString"])


def build_engine():
    creds = get_db_credentials()

    username = creds["username"]
    password = creds["password"]
    host = "itam-northwind-864475311845.c45igc4gw9s5.us-east-1.rds.amazonaws.com"
    port = creds["port"]
    dbname = creds["dbname"]

    conn_str = (
        f"postgresql+psycopg2://{username}:{password}"
        f"@{host}:{port}/{dbname}"
    )
    return create_engine(conn_str, future=True)

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [col.strip().lower() for col in df.columns]
    return df


def clean_nan_to_none(df: pd.DataFrame) -> pd.DataFrame:
    return df.astype(object).where(pd.notnull(df), None)


def load_airlines(session: Session) -> None:
    logger.info("Loading airlines.csv")
    df = pd.read_csv(DATA_DIR / "airlines.csv")
    df = normalize_columns(df)
    df = clean_nan_to_none(df)

    records = df.to_dict(orient="records")
    assert records, "airlines.csv is empty"

    session.execute(text("TRUNCATE TABLE flights RESTART IDENTITY CASCADE"))
    session.execute(text("TRUNCATE TABLE airports CASCADE"))
    session.execute(text("TRUNCATE TABLE airlines CASCADE"))
    session.execute(insert(Airline), records)
    logger.info("Inserted %s airline rows", len(records))


def load_airports(session: Session) -> None:
    logger.info("Loading airports.csv")
    df = pd.read_csv(DATA_DIR / "airports.csv")
    df = normalize_columns(df)
    df = clean_nan_to_none(df)

    records = df.to_dict(orient="records")
    assert records, "airports.csv is empty"

    session.execute(insert(Airport), records)
    logger.info("Inserted %s airport rows", len(records))


def load_flights(session: Session) -> None:
    logger.info("Loading first %s rows from flights.csv", NROWS_FLIGHTS)
    df = pd.read_csv(DATA_DIR / "flights.csv", nrows=NROWS_FLIGHTS, low_memory=False)
    df = normalize_columns(df)

    keep_cols = [
        "year",
        "month",
        "day",
        "day_of_week",
        "airline",
        "flight_number",
        "tail_number",
        "origin_airport",
        "destination_airport",
        "scheduled_departure",
        "departure_time",
        "departure_delay",
        "scheduled_arrival",
        "arrival_time",
        "arrival_delay",
        "scheduled_time",
        "elapsed_time",
        "air_time",
        "distance",
        "wheels_off",
        "taxi_out",
        "wheels_on",
        "taxi_in",
        "diverted",
        "cancelled",
        "cancellation_reason",
        "air_system_delay",
        "security_delay",
        "airline_delay",
        "late_aircraft_delay",
        "weather_delay",
    ]

    df = df[keep_cols]
    df = clean_nan_to_none(df)

    records = df.to_dict(orient="records")
    assert records, "flights.csv produced no rows"

    chunk_size = 10_000
    total = len(records)

    for start in range(0, total, chunk_size):
        end = min(start + chunk_size, total)
        batch = records[start:end]
        session.execute(insert(Flight), batch)
        logger.info("Inserted flight rows %s to %s", start + 1, end)


def main() -> None:
    engine = build_engine()
    logger.info("Dropping and recreating tables in PostgreSQL")
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        load_airlines(session)
        load_airports(session)
        load_flights(session)
        session.commit()

    logger.info("PostgreSQL load completed successfully.")


if __name__ == "__main__":
    main()