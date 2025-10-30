# src/li_yuan_pipeline/utils.py
"""Utility functions for Li Yuan Pipeline."""

from datetime import date, datetime


def ce_date_to_roc_string(
        ce_date: str | date | datetime,
        separator: str = "/"
    ) -> str:
    """
    Convert CE date to ROC date string format

    Parameters
    ----------
    ce_date: str | date | datetime
        Date in CE format as string "YYYY-MM-DD", date object, or datetime
        object

    separator: str
        Separator to use in the ROC date string (default is "/")

    Returns
    -------
    str
        ROC date string in format "YYY/MM/DD" or others based on specified
        separator
    """
    if isinstance(ce_date, str):
        ce_date = datetime.strptime(ce_date, "%Y-%m-%d").date()
    elif isinstance(ce_date, datetime):
        ce_date = ce_date.date()

    roc_year = ce_date.year - 1911
    return f"{roc_year:03d}{separator}{ce_date.month:02d}{separator}{ce_date.day:02d}"


def roc_string_to_ce_date(roc_date_str: str) -> date:
    """
    Convert ROC date string to CE date object

    Parameters
    ----------
    roc_date_str: str
        ROC date string in format "YYY/MM/DD" or "YYY-MM-DD"

    Returns
    -------
    date
        Python date object in CE format
    """
    # Handle both "/" and "-" separators
    separator = "/" if "/" in roc_date_str else "-"
    parts = roc_date_str.split(separator)

    roc_year = int(parts[0])
    month = int(parts[1])
    day = int(parts[2])

    ce_year = roc_year + 1911
    return date(ce_year, month, day)
