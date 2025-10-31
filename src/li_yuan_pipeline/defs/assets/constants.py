# src/li_yuan_pipeline/defs/assets/constants.py
"""Constants for Li Yuan Pipeline assets."""

LI_YUAN_URL = "https://www.ly.gov.tw/WebAPI/LegislativeSpeech.aspx"

START_DATE = "2023-01-01"
END_DATE = "2023-12-31"

SPEECH_DATA_FILEPATH_TEMPLATE = "data/raw/speeches_{partition}.parquet"
SPEECH_COUNT_FILEPATH = "data/staging/speech_counts.csv"
SPEECH_BAR_CHART_FILEPATH = "data/output/speech_count_bar_chart.png"
