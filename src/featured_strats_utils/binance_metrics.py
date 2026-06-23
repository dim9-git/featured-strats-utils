from dataclasses import dataclass
from datetime import date, timedelta
from io import BytesIO
from typing import Iterator
from zipfile import ZipFile

import pandas as pd
import requests

from .fetching_and_caching import fetch_with_cache, get_df_cache_path

BASE_URL = "https://data.binance.vision/data/futures/um/daily/metrics"
DAY_MS = 86_400_000


@dataclass(frozen=True)
class BinanceMetricsParams:
    symbol: str   # raw Binance symbol, e.g. BTCUSDT
    start: str    # YYYY-MM-DD
    end: str      # exclusive


def metrics_zip_url(symbol: str, day: date) -> str:
    day_str = day.strftime("%Y-%m-%d")
    return f"{BASE_URL}/{symbol}/{symbol}-metrics-{day_str}.zip"


def download_metrics_day(
    symbol: str,
    day: date,
    session: requests.Session,
) -> pd.DataFrame | None:
    url = metrics_zip_url(symbol, day)
    resp = session.get(url, timeout=60)

    if resp.status_code == 404:
        return None
    resp.raise_for_status()

    with ZipFile(BytesIO(resp.content)) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            return None
        with zf.open(csv_names[0]) as f:
            df = pd.read_csv(f)

    time_col = "create_time" if "create_time" in df.columns else "timestamp"
    df[time_col] = pd.to_datetime(df[time_col], utc=True)
    df = df.set_index(time_col).sort_index()
    df = df.rename(columns={
        "sum_open_interest": "open_interest",
        "sum_open_interest_value": "open_interest_value",
    })

    numeric_cols = [
        "open_interest",
        "open_interest_value",
        "count_toptrader_long_short_ratio",
        "sum_toptrader_long_short_ratio",
        "count_long_short_ratio",
        "sum_taker_long_short_vol_ratio",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def paginate_binance_metrics(
    params: BinanceMetricsParams,
    *,
    since_ms: int,
    end_day: date,
) -> Iterator[pd.DataFrame]:
    session = requests.Session()
    session.headers.update({"User-Agent": "regime-change-metrics-fetch/1.0"})

    day = pd.Timestamp(since_ms, unit="ms", tz="UTC").normalize().date()

    while day < end_day:
        day_df = download_metrics_day(params.symbol, day, session)
        if day_df is not None and not day_df.empty:
            yield day_df
        day += timedelta(days=1)


def fetch_binance_metrics_df(params: BinanceMetricsParams) -> pd.DataFrame:
    start_day = date.fromisoformat(params.start)
    end_day = date.fromisoformat(params.end)

    cache_path = get_df_cache_path(
        params.symbol,
        params.start,
        params.end,
        timeframe="1d",
        prefix="binance_metrics",
    )

    since_ms = int(pd.Timestamp(start_day, tz="UTC").timestamp() * 1000)

    return fetch_with_cache(
        cache_path,
        since_ms=since_ms,
        tf_ms=DAY_MS,
        paginate=lambda start_ms: paginate_binance_metrics(
            params,
            since_ms=start_ms,
            end_day=end_day,
        ),
        empty_error=(
            f"No metrics files found for {params.symbol} "
            f"between {params.start} and {params.end}."
        ),
    )