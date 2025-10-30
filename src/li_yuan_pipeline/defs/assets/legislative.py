# src/li_yuan_pipeline/defs/assets/legislative.py
"""Asset definition for ingested data from the legislative yuan's API."""

from datetime import date, timedelta

import dagster as dg
import polars as pl

from li_yuan_pipeline.defs.assets.constants import SPEECH_DATA_PATH_TEMPLATE
from li_yuan_pipeline.defs.partitions import monthly_partitions
from li_yuan_pipeline.defs.resources import OpenAPIResource
from li_yuan_pipeline.utils import ce_date_to_roc_string, roc_string_to_ce_date


@dg.asset(partitions_def=monthly_partitions, kinds={"python"})
def speech_data(context: dg.AssetExecutionContext, li_yuan_api: OpenAPIResource) -> None:
    """Asset representing speech data from the legislative yuan's API."""
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
        context.log.info(
            f"No speech data found for partition {partition_date_string}."
        )
        return None
    df = pl.DataFrame(data)
    df = (
        df.rename(
            {
                "smeeting_date": "meeting_date",
                "speechers": "speakers",
            }
        )
        .with_columns(
            pl.col("meeting_date").map_elements(
                roc_string_to_ce_date, return_dtype=pl.Date,
            ),
            pl.col("speakers")
            .fill_null("")
            .str.replace_all(r"\d+", "")
            .str.replace_all(r"\s+", " ")
            .str.strip_chars(", ")
            .str.split(",")
            .map_elements(
                lambda lst: [name.strip() for name in lst if name.strip()],
                return_dtype=pl.List(pl.String),
            )
            .alias("speakers_list")
        )
    )
    df.write_parquet(SPEECH_DATA_PATH_TEMPLATE.format(partition=partition_date_string))

    return None
