from dataclasses import dataclass
from typing import Literal
from pathlib import Path

import pandas as pd
import ccxt

from .dataframe import ensure_datetime_index

@dataclass(frozen=True)
class CcxtParams:
    exchange_id: Literal["binance", 'kucoin', 'okx', 'bybit']
    symbol: str
    timeframe: str
    start: str # YYYY-MM-DD (UTC)
    end: str # exclusive-ish; set to 2025-01-01 to cover 2024
    limit: int = 1000


def fetch_ccxt_df(params: CcxtParams) -> pd.DataFrame:
    ex_attr = getattr(ccxt, params.exchange_id)
    ex = ex_attr({
        "enableRateLimit": True,
        "options": {"fetchMarkets": ["spot"]},
    })

    tf_ms = int(ex.parse_timeframe(params.timeframe) * 1000)
    end_ms = ex.parse8601(f"{params.end}T00:00:00Z") if params.end else None

    # Changeable
    since_ms = ex.parse8601(f"{params.start}T00:00:00Z")
    next_part_id = 0

    cache_path = cache_parquet_path(params)

    # Early return
    if cache_path.exists():
        df = pd.read_parquet(cache_path)
        df = ensure_datetime_index(df)
        return df

    part_dir = cache_path.parent / ".inprogress" / cache_path.stem
    part_dir.mkdir(parents=True, exist_ok=True)

    part_paths = sorted(part_dir.glob("part_*.parquet"))
    if part_paths:
        last_part_df = pd.read_parquet(part_paths[-1])
        last_part_df = ensure_datetime_index(last_part_df)
        last_ms = last_part_df.index.max().value // 1_000_000

        since_ms = last_ms + tf_ms
        next_part_id = len(part_paths)

    while end_ms is None or since_ms < end_ms:
        batch = ex.fetch_ohlcv(params.symbol, timeframe=params.timeframe, since=since_ms, limit=params.limit)
        if not batch:
            break

        batch_df = convert_bars_to_df(batch)

        part_path = part_dir / f"part_{next_part_id:05d}.parquet"
        batch_df.to_parquet(part_path, index=True)
        part_paths.append(part_path)
        next_part_id += 1

        last_ms = batch[-1][0]
        since_ms = last_ms + tf_ms

        if len(batch) < params.limit:
            break

    if not part_paths:
        part_dir.rmdir()
        raise ValueError(
            "No OHLCV data fetched. Check symbol, date range, and API availability."
        )

    dfs = [pd.read_parquet(part_path) for part_path in part_paths]
    df = pd.concat(dfs, axis=0)
    df = ensure_datetime_index(df)
    df = df[~df.index.duplicated(keep="last")].sort_index()
    df.to_parquet(cache_path, index=True)
    for p in part_paths:
        p.unlink(missing_ok=True)
    part_dir.rmdir()
    return df

def convert_bars_to_df(batch: list[list]) -> pd.DataFrame:
    ohlcv_cols = ["Open", "High", "Low", "Close", "Volume"]
    df = pd.DataFrame(batch, columns=["Date", *ohlcv_cols])
    df[ohlcv_cols] = df[ohlcv_cols].astype(float)
    df["Date"] = pd.to_datetime(df["Date"], unit="ms", utc=True)
    df = df.set_index("Date")
    return df

def cache_parquet_path(params: CcxtParams) -> Path:
    cache_dir = Path('../cache')
    cache_dir.mkdir(parents=True, exist_ok=True)
    symbol = params.symbol.replace("/", "-").replace(":", "-")
    file_name = f"{params.exchange_id}_{symbol}_{params.timeframe}_{params.start}_{params.end}.parquet"
    return cache_dir / file_name
