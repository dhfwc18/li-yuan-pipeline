# src/li_yuan_pipeline/defs/assets/metrics.py
"""Asset definition for metrics on legislative data."""

import dagster as dg
import matplotlib.pyplot as plt
import pandas as pd

from li_yuan_pipeline.defs.assets.constants import (
    SPEECH_BAR_CHART_FILEPATH,
    SPEECH_COUNT_FILEPATH,
)
from li_yuan_pipeline.defs.resources import DuckDBResource


@dg.asset(deps=["speech_data", "speaker_data"], kinds={"duckdb"})
def speech_count(
    context: dg.AssetExecutionContext,
    main_database: DuckDBResource,
) -> None:
    """Speech count per speaker data fetched from the duckdb database."""
    query = """
    SELECT
        s.speaker as speaker,
        COUNT(*) AS speech_count
    FROM speech_data AS d
    JOIN speaker_data AS s
        ON d.speaker_id = s.speaker_id
    GROUP BY s.speaker
    ORDER BY speech_count DESC;
    """
    with main_database.get_connection() as conn:
        df = conn.execute(query).pl()

    context.log.debug(f"Computed speech counts for {df.height} speakers.")

    df.write_csv(SPEECH_COUNT_FILEPATH)
    return None


@dg.asset(deps=["speech_count"], kinds={"python"})
def speech_count_bar_chart(
    context: dg.AssetExecutionContext,
) -> None:
    """Bar chart of speech counts per speaker."""
    df = pd.read_csv(SPEECH_COUNT_FILEPATH)

    # Set up font for Chinese characters
    plt.rcParams["font.family"] = "Microsoft JhengHei"
    plt.rcParams["axes.unicode_minus"] = False

    # Plot
    plt.figure(figsize=(10, 6))
    plt.barh(df["speaker"], df["speech_count"], color="skyblue", edgecolor="black")
    plt.xlabel("Number of Speeches", fontsize=12)
    plt.ylabel("Speaker", fontsize=12)
    plt.title("Top 20 Speakers by Number of Speeches", fontsize=14)
    plt.gca().invert_yaxis()  # highest count on top
    plt.grid(axis="x", linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.savefig(SPEECH_BAR_CHART_FILEPATH)
    context.log.info(f"Speech counts metric saved to {SPEECH_BAR_CHART_FILEPATH}.")
    return None
