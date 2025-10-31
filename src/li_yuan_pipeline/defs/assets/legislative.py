# src/li_yuan_pipeline/defs/assets/legislative.py
"""Asset definition for ingested data from the legislative yuan's API."""

from datetime import date, timedelta

import dagster as dg
import polars as pl

from li_yuan_pipeline.defs.assets.constants import SPEECH_DATA_FILEPATH_TEMPLATE
from li_yuan_pipeline.defs.partitions import monthly_partitions
from li_yuan_pipeline.defs.resources import DuckDBResource, OpenAPIResource
from li_yuan_pipeline.utils import ce_date_to_roc_string, roc_string_to_ce_date


@dg.asset(partitions_def=monthly_partitions, kinds={"python"})
def speech_file(
    context: dg.AssetExecutionContext, li_yuan_api: OpenAPIResource
) -> None:
    """
    Speech datasets downloaded from Legislative Yuan API and saved as parquet.
    """
    partition_date_string = context.partition_key
    one_month_date = date.fromisoformat(partition_date_string) + timedelta(days=31)
    start_date_roc = ce_date_to_roc_string(ce_date=partition_date_string, separator="")
    end_date_roc = ce_date_to_roc_string(ce_date=one_month_date, separator="")
    params = {
        "from": start_date_roc,
        "to": end_date_roc,
        "meeting_unit": "內政委員會",
        "mode": "JSON",
    }
    data = li_yuan_api.get(endpoint="LegislativeSpeech.aspx", params=params)
    if len(data) == 0:
        context.log.info(f"No speech data found for partition {partition_date_string}.")
        return None
    df = (
        pl.DataFrame(data)
        .rename(
            {
                "smeeting_date": "meeting_date",
                "speechers": "speakers",
            }
        )
        .with_columns(
            pl.col("meeting_date").map_elements(
                roc_string_to_ce_date, return_dtype=pl.Date
            ),
            pl.col("speakers")
            .fill_null("")
            .str.strip_chars(", ")  # remove trailing commas/spaces
            .str.strip_chars(" ")  # trim spaces
            .alias("speakers_clean"),
            pl.lit(partition_date_string).alias("partition_date"),
        )
        .with_columns(pl.col("speakers_clean").str.split(",").alias("speakers_split"))
        .explode("speakers_split")
        .with_columns(pl.col("speakers_split").str.strip_chars(" ").alias("speaker"))
        .drop(["speakers", "speakers_clean", "speakers_split"])
        .with_columns(
            pl.col("speaker")
            .str.extract(r"(\d+)", 1)
            .cast(pl.UInt32)
            .alias("speaker_id"),
            pl.col("speaker")
            .str.replace_all(r"\d+", "")  # remove numbers
            .str.replace_all(r"\s+", " ")  # normalize spaces
            .str.strip_chars(" ")  # trim spaces
            .alias("speaker"),
        )
    )
    df.write_parquet(
        SPEECH_DATA_FILEPATH_TEMPLATE.format(partition=partition_date_string)
    )

    return None


@dg.asset(
    deps=["speech_file"],
    partitions_def=monthly_partitions,
    kinds={"duckdb"},
)
def speech_data(
    context: dg.AssetExecutionContext,
    main_database: DuckDBResource,
) -> None:
    """
    Speech data loaded into DuckDB from parquet files.
    Each partition corresponds to one month of speech data.
    """

    # Partition setup
    partition_date_string = context.partition_key
    path = SPEECH_DATA_FILEPATH_TEMPLATE.format(partition=partition_date_string)

    # Load Parquet
    try:
        df = pl.read_parquet(path)
        context.log.info(f"Loaded {df.height} rows from {path}")
    except FileNotFoundError:
        context.log.warning(f"Parquet file not found at {path}. No data to load.")
        return None

    # Expected column order for DuckDB table
    expected_cols = [
        "meeting_date",
        "meeting_status",
        "meeting_name",
        "meeting_content",
        "meeting_unit",
        "speaker_id",
        "partition_date",
    ]

    # Ensure all expected columns exist
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in dataframe: {missing}")

    # Reorder Polars DF to match DuckDB schema order
    df = df.select(expected_cols)

    context.log.debug(f"Polars columns (ordered): {df.columns}")

    # Write into DuckDB
    with main_database.get_connection() as conn:
        # Ensure table exists
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS speech_data (
                meeting_date DATE,
                meeting_status TEXT,
                meeting_name TEXT,
                meeting_content TEXT,
                meeting_unit TEXT,
                speaker_id INTEGER,
                partition_date DATE
            );
            """
        )

        # Delete any rows from this partition
        conn.execute(
            f"DELETE FROM speech_data WHERE partition_date = '{partition_date_string}';"
        )
        context.log.debug(
            f"Cleared existing rows for partition {partition_date_string}."
        )

        # Register DataFrame temporarily
        conn.register("tmp_speech", df)

        # Insert rows
        conn.execute(
            """
            INSERT INTO speech_data (
                meeting_date,
                meeting_status,
                meeting_name,
                meeting_content,
                meeting_unit,
                speaker_id,
                partition_date
            )
            SELECT
                meeting_date,
                meeting_status,
                meeting_name,
                meeting_content,
                meeting_unit,
                speaker_id,
                partition_date
            FROM tmp_speech;
            """
        )

        context.log.info(
            f"Inserted {df.height} rows for partition {partition_date_string} into DuckDB."
        )

    return None


@dg.asset(deps=["speech_file"], partitions_def=monthly_partitions, kinds={"duckdb"})
def speaker_data(
    context: dg.AssetExecutionContext, main_database: DuckDBResource
) -> None:
    """Unique speaker data, loaded into DuckDB from speech data."""
    partition_date_string = context.partition_key
    path = SPEECH_DATA_FILEPATH_TEMPLATE.format(partition=partition_date_string)

    # Load Parquet
    try:
        df = pl.read_parquet(path)
        context.log.info(f"Loaded {df.height} rows from {path}")
    except FileNotFoundError:
        context.log.warning(f"Parquet file not found at {path}. No data to load.")
        return None

    # Extract unique speakers
    df = df.select(
        [
            pl.col("speaker_id"),
            pl.col("speaker"),
        ]
    ).unique()
    df = df.drop_nulls(subset=["speaker_id"])

    if df.is_empty():
        context.log.info(f"No speaker data found in partition {partition_date_string}.")
        return None

    context.log.debug(f"Unique speakers extracted: {df.height} rows.")

    # Write into DuckDB
    with main_database.get_connection() as conn:
        # Ensure table exists
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS speaker_data (
                speaker_id INTEGER PRIMARY KEY,
                speaker TEXT
            );
            """
        )

        # Insert or update speakers
        before_count = conn.execute("SELECT COUNT(*) FROM speaker_data").fetchone()[0]
        conn.register("tmp_speakers", df)

        conn.execute(
            """
            INSERT INTO speaker_data (speaker_id, speaker)
            SELECT speaker_id, speaker FROM tmp_speakers
            ON CONFLICT (speaker_id) DO UPDATE SET
                speaker = EXCLUDED.speaker;
            """
        )

        after_count = conn.execute("SELECT COUNT(*) FROM speaker_data").fetchone()[0]

        new_count = after_count - before_count
        context.log.info(
            f"Upserted {df.height} total speakers "
            f"({new_count} new, {df.height - new_count} existing updated)."
        )

    return None
