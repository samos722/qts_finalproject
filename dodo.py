# doit entry: doit (full pipeline) / doit fetch (download only) / doit align (build panel only)
"""
doit: run `doit` to fetch data + build panel.
  doit          - fetch + align (data pipeline)
  doit fetch    - fetch raw data only
  doit fetch_test - quick test (2 symbols, 1 month)
  doit align    - build panel from raw data
"""

from pathlib import Path

from src.config import DATA_DIR, OUTPUT_DIR, START_DATE, END_DATE

K1H = str(DATA_DIR / "klines_1h.parquet")
K5M = str(DATA_DIR / "klines_5m.parquet")
FUND = str(DATA_DIR / "funding_8h.parquet")
PANEL = str(DATA_DIR / "panel_1h.parquet")


def do_fetch():
    from src.data_fetch import build_local_dataset
    build_local_dataset({"start": START_DATE, "end": END_DATE}, DATA_DIR)


def do_fetch_test():
    from src.data_fetch import build_local_dataset
    build_local_dataset({"start": "2024-01-01", "end": "2024-01-31", "symbols": ["BTCUSDT", "ETHUSDT"]}, DATA_DIR)


def do_align():
    from src.align import align_to_panel
    panel = align_to_panel(DATA_DIR)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(DATA_DIR / "panel_1h.parquet", index=False)


def task_fetch():
    return {"actions": [do_fetch], "targets": [K1H, K5M, FUND], "file_dep": ["src/data_fetch.py", "src/config.py"], "clean": True}


def task_fetch_test():
    """Quick test: 2 symbols, 1 month. Run manually: doit fetch_test."""
    return {"actions": [do_fetch_test], "targets": [], "file_dep": ["src/data_fetch.py", "src/config.py"]}


def task_align():
    return {"actions": [do_align], "targets": [PANEL], "file_dep": [K1H, FUND, "src/align.py"], "clean": True}
