import pandas as pd
import numpy as np
import requests
from pathlib import Path

def ensure_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df.index, pd.DatetimeIndex):
        return df.reset_index().rename(columns={"index": "Date"}).set_index("Date")
    return df


def resample_metrics_daily(df: pd.DataFrame) -> pd.DataFrame:
    """Last 5m OI sample of each UTC day — good match for memlabs 1d BTC CSV."""
    return df.resample("1D").last()


def merge_btc_price_and_oi(
    price_df: pd.DataFrame,
    oi_daily: pd.DataFrame,
    price_date_col: str = "t",
) -> pd.DataFrame:
    """
    price_df: memlabs BTCUSDT-1d.csv with columns t, c, ...
    oi_daily: output of resample_metrics_daily()
    """
    btc = price_df.copy()
    btc[price_date_col] = pd.to_datetime(btc[price_date_col], utc=True)
    btc = btc.set_index(price_date_col)

    merged = btc.join(
        oi_daily[["open_interest", "open_interest_value"]],
        how="inner",
    )
    return merged


def color_return(val):
    if pd.isna(val):
        return ""
    if val > 0:
        return "color: green; font-weight: 600"
    if val < 0:
        return "color: red; font-weight: 600"
    return "color: gray"


def sparse_cooldown(mask: pd.Series, cooldown: int) -> pd.Series:
    pos = np.flatnonzero(mask.fillna(False).to_numpy())
    out = np.zeros(len(mask), dtype=bool)
    last_kept = -10**9
    for p in pos:
        if p - last_kept >= cooldown:
            out[p] = True
            last_kept = p
    return pd.Series(out, index=mask.index)

def rolling_zscore(series: pd.Series, window: int) -> pd.Series:
    mean = series.rolling(window).mean()
    std = series.rolling(window).std()
    return (series - mean) / std


def fetch_json_with_retries(url: str, retries: int = 3, timeout: int = 120) -> dict:
    last_exc: Exception | None = None
    for _ in range(retries):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            last_exc = exc
    raise RuntimeError(f"Failed to fetch JSON after {retries} attempts: {url}") from last_exc


def load_daily_json_data(url: str, column_name: str) -> pd.DataFrame:
    cache_dir = Path("data/daily")
    cache_dir.mkdir(parents=True, exist_ok=True)

    dataset_name = url.split("/")[-1].split(".")[0]
    file_source = cache_dir / f"{dataset_name}.csv"

    if file_source.exists():
        out = pd.read_csv(file_source, parse_dates=["Date"], index_col="Date")
        if out.index.tz is None:
            out.index = out.index.tz_localize("UTC")
        return out.sort_index()

    payload = fetch_json_with_retries(url)

    raw = pd.DataFrame(payload["data"])
    out = clean_daily_data(raw, column_name)

    first_date = out.index[0].strftime("%Y-%m-%d")
    last_date = out.index[-1].strftime("%Y-%m-%d")

    out.to_csv(cache_dir / f"{dataset_name}.csv_{first_date}__{last_date}.csv")
    out.to_csv(file_source)

    return out

def clean_daily_data(raw: pd.DataFrame, column_name: str) -> pd.DataFrame:
    out = raw[["timestamp", "value"]].copy()
    out.columns = ["Date", column_name]

    out["Date"] = pd.to_datetime(out["Date"], unit="ms", utc=True)
    out[column_name] = pd.to_numeric(out[column_name], errors="coerce")

    return out.dropna().set_index("Date").sort_index()