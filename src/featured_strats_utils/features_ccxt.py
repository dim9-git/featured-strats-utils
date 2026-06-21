from typing import Literal
from dataclasses import dataclass
from pathlib import Path

import ccxt
import pandas as pd

from .dataframe import ensure_datetime_index


@dataclass(frozen=True)
class CcxtOiParams:
    exchange_id: Literal["binance", "bybit", "okx"]
    symbol: str              # e.g. "BTC/USDT:USDT"
    timeframe: str           # "5m" … "1d" (not "1m" on Binance)
    start: str               # YYYY-MM-DD UTC
    end: str                 # exclusive-ish, same convention as CcxtParams
    limit: int = 500         # Binance max for openInterestHist
    market_type: Literal["future", "swap"] = "swap"

def _make_exchange(exchange_id: str, market_type: str):
    ex_attr = getattr(ccxt, exchange_id)
    return ex_attr({
        "enableRateLimit": True,
        "options": {
            "defaultType": market_type,
            "fetchMarkets": ["linear", "inverse"],
        },
    })

def oi_cache_parquet_path(params: CcxtOiParams) -> Path:
    cache_dir = Path("../cache")
    cache_dir.mkdir(parents=True, exist_ok=True)
    symbol = params.symbol.replace("/", "-").replace(":", "-")
    file_name = (
        f"oi_{params.exchange_id}_{symbol}_{params.timeframe}_"
        f"{params.start}_{params.end}.parquet"
    )
    return cache_dir / file_name

def convert_oi_bars_to_df(batch: list[dict]) -> pd.DataFrame:
    rows = []
    for bar in batch:
        rows.append({
            "Date": bar["timestamp"],
            "open_interest": bar.get("openInterestAmount"),
            "open_interest_value": bar.get("openInterestValue"),
            "symbol": bar.get("symbol"),
        })
    df = pd.DataFrame(rows)
    df["Date"] = pd.to_datetime(df["Date"], unit="ms", utc=True)
    df[["open_interest", "open_interest_value"]] = df[
        ["open_interest", "open_interest_value"]
    ].astype(float)
    return df.set_index("Date")

def fetch_open_interest_df(params: CcxtOiParams) -> pd.DataFrame:
    ex = _make_exchange(params.exchange_id, params.market_type)
    ex.load_markets()
    tf_ms = int(ex.parse_timeframe(params.timeframe) * 1000)
    since_ms = ex.parse8601(f"{params.start}T00:00:00Z")
    end_ms = ex.parse8601(f"{params.end}T00:00:00Z") if params.end else None
    cache_path = oi_cache_parquet_path(params)
    if cache_path.exists():
        return ensure_datetime_index(pd.read_parquet(cache_path))
    part_dir = cache_path.parent / ".inprogress" / cache_path.stem
    part_dir.mkdir(parents=True, exist_ok=True)
    part_paths = sorted(part_dir.glob("part_*.parquet"))
    next_part_id = 0
    if part_paths:
        last_part_df = ensure_datetime_index(pd.read_parquet(part_paths[-1]))
        since_ms = last_part_df.index.max().value // 1_000_000 + tf_ms
        next_part_id = len(part_paths)
    while end_ms is None or since_ms < end_ms:
        batch = ex.fetch_open_interest_history(
            params.symbol,
            timeframe=params.timeframe,
            since=since_ms,
            limit=params.limit,
            params={"until": end_ms} if end_ms else {},
        )
        if not batch:
            break
        batch_df = convert_oi_bars_to_df(batch)
        part_path = part_dir / f"part_{next_part_id:05d}.parquet"
        batch_df.to_parquet(part_path, index=True)
        part_paths.append(part_path)
        next_part_id += 1
        last_ms = batch[-1]["timestamp"]
        since_ms = last_ms + tf_ms
        if len(batch) < params.limit:
            break
    if not part_paths:
        part_dir.rmdir()
        raise ValueError(
            "No open-interest data fetched. Check symbol, timeframe, "
            "exchange support, and the ~30-day Binance window."
        )
    df = pd.concat(
        [ensure_datetime_index(pd.read_parquet(p)) for p in part_paths],
        axis=0,
    )
    df = df[~df.index.duplicated(keep="last")].sort_index()
    df.to_parquet(cache_path, index=True)
    for p in part_paths:
        p.unlink(missing_ok=True)
    part_dir.rmdir()
    return df

def fetch_open_interest_snapshot(
    exchange_id: str,
    symbol: str,
    market_type: Literal["future", "swap"] = "swap",
) -> dict:
    ex = _make_exchange(exchange_id, market_type)
    ex.load_markets()
    return ex.fetch_open_interest(symbol)