# src/li_yuan_pipeline/defs/resources.py
"""Resource definitions for Li Yuan Pipeline."""

from typing import Any

import dagster as dg
import requests
from dagster_duckdb import DuckDBResource

from li_yuan_pipeline.defs.assets.constants import LI_YUAN_URL


class OpenAPIResource(dg.ConfigurableResource):
    """Resource for interacting with Open APIs."""

    base_url: str
    api_key: str | None = None
    verify_ssl: bool = True
    default_headers: dict[str, str] = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate,",
        "Connection": "keep-alive",
        "Referer": "https://www.ly.gov.tw/",
        "Origin": "https://www.ly.gov.tw",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
    }
    timeout: int = 30

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        """Initialise the underlying requests.Session once per run."""
        self._session = requests.Session()
        headers = self.default_headers
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        self._session.headers.update(headers)

    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Perform GET request and safely decode JSON response."""
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        response = self._session.get(
            url, params=params, timeout=self.timeout, verify=self.verify_ssl
        )
        response.raise_for_status()
        return response.json()

    def post(self, endpoint: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
        """Perform POST request."""
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        response = self._session.post(url, json=data, timeout=self.timeout)
        response.raise_for_status()
        return response.json()


main_database = DuckDBResource(database=dg.EnvVar("MAIN_DUCKDB_DATABASE"))


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "li_yuan_api": OpenAPIResource(base_url=LI_YUAN_URL, verify_ssl=False),
            "main_database": main_database,
        }
    )
