from pathlib import Path

def get_cache_parquet_path(filename: str, prefix: None | str = None) -> Path:
    cache_dir = Path('cache')
    cache_dir.mkdir(parents=True, exist_ok=True)
    final = f'{prefix}_{filename}' if prefix else filename
    return cache_dir / final

def get_filename_for_parquet(
        symbol: str,
        start: str,
        end: str,
        timeframe: str,
        exchange_id: str | None = None,
) -> str:
    asset_symbol = symbol.replace("/", "-").replace(":", "-")
    filename = f"{asset_symbol}_{timeframe}_{start}_{end}.parquet"
    filename = f"{exchange_id}_{filename}" if exchange_id else filename
    return filename
