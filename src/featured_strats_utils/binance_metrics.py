from dataclasses import dataclass
from pathlib import Path
from datetime import date, timedelta
from io import BytesIO
from zipfile import ZipFile
import requests

import pandas as pd

BASE_URL = "https://data.binance.vision/data/futures/um/daily/metrics"

@dataclass(frozen=True)
class BinanceMetricsParams:
    symbol: str = "BTCUSDT"          # raw Binance symbol, not CCXT unified
    start: str = "2020-01-01"        # YYYY-MM-DD
    end: str = "2026-06-07"          # exclusive, same convention as CcxtParams
    cache_dir: Path = Path("../cache")


def metrics_zip_url(symbol: str, day: date) -> str:
    day_str = day.strftime("%Y-%m-%d")
    return f"{BASE_URL}/{symbol}/{symbol}-metrics-{day_str}.zip"


def metrics_cache_path(params: BinanceMetricsParams) -> Path:
    params.cache_dir.mkdir(parents=True, exist_ok=True)
    return (
        params.cache_dir
        / f"binance_metrics_{params.symbol}_{params.start}_{params.end}.parquet"
    )


def _download_metrics_day(symbol: str, day: date, session: requests.Session) -> pd.DataFrame | None:
    url = metrics_zip_url(symbol, day)
    resp = session.get(url, timeout=60)

    if resp.status_code == 404:
        return None  # no file for that day (pre-launch, outage, etc.)
    resp.raise_for_status()

    with ZipFile(BytesIO(resp.content)) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            return None
        with zf.open(csv_names[0]) as f:
            df = pd.read_csv(f)

    # Binance columns (5m samples inside each daily file):
    # create_time, symbol, sum_open_interest, sum_open_interest_value,
    # count_toptrader_long_short_ratio, sum_toptrader_long_short_ratio,
    # count_long_short_ratio, sum_taker_long_short_vol_ratio

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


def fetch_binance_metrics_df(params: BinanceMetricsParams) -> pd.DataFrame:
    """
    Download BTCUSDT (or other UM perp) metrics from data.binance.vision.
    Returns 5-minute OI + long/short ratio columns.
    """
    cache_path = metrics_cache_path(params)
    if cache_path.exists():
        df = pd.read_parquet(cache_path)
        if not isinstance(df.index, pd.DatetimeIndex):
            df = df.set_index("Date")
        return df.sort_index()

    start_day = date.fromisoformat(params.start)
    end_day = date.fromisoformat(params.end)  # exclusive

    part_dir = cache_path.parent / ".inprogress" / cache_path.stem
    part_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": "regime-change-metrics-fetch/1.0"})

    day = start_day
    while day < end_day:
        part_path = part_dir / f"{day.isoformat()}.parquet"
        if not part_path.exists():
            day_df = _download_metrics_day(params.symbol, day, session)
            if day_df is not None and not day_df.empty:
                day_df.to_parquet(part_path, index=True)
        day += timedelta(days=1)

    part_paths = sorted(part_dir.glob("*.parquet"))
    if not part_paths:
        raise ValueError(
            f"No metrics files found for {params.symbol} "
            f"between {params.start} and {params.end}."
        )

    df = pd.concat(
        [pd.read_parquet(p) for p in part_paths],
        axis=0,
    )
    df = df[~df.index.duplicated(keep="last")].sort_index()
    df.to_parquet(cache_path, index=True)
    return df