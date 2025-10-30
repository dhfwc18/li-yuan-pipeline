# src/li_yuan_pipeline/defs/partitions.py
"""Partition definitions for Li Yuan Pipeline."""

import dagster as dg

from li_yuan_pipeline.defs.assets.constants import END_DATE, START_DATE

monthly_partitions = dg.MonthlyPartitionsDefinition(
    start_date=START_DATE,
    end_date=END_DATE,
)
