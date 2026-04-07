from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import awswrangler as wr
import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


BRONZE_DATABASE = "flights_bronze"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload raw flights CSV files to S3 Bronze layer and register them in Glue."
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="Target S3 bucket name.",
    )
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Local directory containing flights.csv, airlines.csv, and airports.csv.",
    )
    return parser.parse_args()


def validate_file_exists(file_path: Path) -> None:
    assert file_path.exists(), f"File does not exist: {file_path}"
    assert file_path.is_file(), f"Path is not a file: {file_path}"


def read_csv_file(file_path: Path) -> pd.DataFrame:
    logger.info("Reading file: %s", file_path)
    df = pd.read_csv(file_path)

    assert not df.empty, f"DataFrame is empty for file: {file_path.name}"
    assert len(df.columns) > 0, f"No columns found in file: {file_path.name}"

    return df


def validate_dataframe(df: pd.DataFrame, table_name: str) -> None:
    assert not df.empty, f"{table_name} DataFrame is empty."

    if table_name == "flights":
        required_columns = [
            "YEAR",
            "MONTH",
            "DAY",
            "AIRLINE",
            "ORIGIN_AIRPORT",
            "DESTINATION_AIRPORT",
        ]
        for column in required_columns:
            assert column in df.columns, f"Missing required column '{column}' in {table_name}."
            assert df[column].notna().all(), f"Column '{column}' contains nulls in {table_name}."

    elif table_name == "airlines":
        required_columns = ["IATA_CODE", "AIRLINE"]
        for column in required_columns:
            assert column in df.columns, f"Missing required column '{column}' in {table_name}."
            assert df[column].notna().all(), f"Column '{column}' contains nulls in {table_name}."

    elif table_name == "airports":
        required_columns = ["IATA_CODE", "AIRPORT", "CITY", "STATE"]
        for column in required_columns:
            assert column in df.columns, f"Missing required column '{column}' in {table_name}."
            assert df[column].notna().all(), f"Column '{column}' contains nulls in {table_name}."


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [column.strip().lower() for column in df.columns]
    return df


def upload_table(
    df: pd.DataFrame,
    bucket: str,
    table_name: str,
) -> None:
    s3_path = f"s3://{bucket}/flights/bronze/{table_name}/"
    logger.info("Writing %s rows to %s", len(df), s3_path)

    wr.s3.to_parquet(
        df=df,
        path=s3_path,
        dataset=True,
        mode="overwrite",
        database=BRONZE_DATABASE,
        table=table_name,
        compression="snappy",
    )

    logger.info(
        "Table '%s' uploaded successfully. Rows: %s | S3 path: %s",
        table_name,
        len(df),
        s3_path,
    )


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir)

    logger.info("Creating Glue database if it does not exist: %s", BRONZE_DATABASE)
    wr.catalog.create_database(name=BRONZE_DATABASE, exist_ok=True)

    files = {
        "flights": data_dir / "flights.csv",
        "airlines": data_dir / "airlines.csv",
        "airports": data_dir / "airports.csv",
    }

    for table_name, file_path in files.items():
        validate_file_exists(file_path)
        df = read_csv_file(file_path)
        validate_dataframe(df, table_name)
        df = normalize_columns(df)
        upload_table(df=df, bucket=args.bucket, table_name=table_name)

    logger.info("Bronze layer completed successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Bronze ETL failed.")
        sys.exit(1)