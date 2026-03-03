# Fetches 1h/5m klines + 8h funding from data.binance.vision → parquet
"""
Fetch data from data.binance.vision only (no REST API).
OI not available -> use volume as crowding proxy.
"""

import io
import time
import zipfile
from pathlib import Path

import pandas as pd
import requests

from .config import SYMBOLS, START_DATE, END_DATE

DATA_VISION_BASE = "https://data.binance.vision/data/futures/um"


def _date_range(start: str, end: str):
    s = pd.Timestamp(start)
    e = pd.Timestamp(end)
    for d in pd.date_range(s, e, freq="D"):
        yield d.strftime("%Y-%m-%d"), d.strftime("%Y-%m")


def _download_zip(url: str, timeout: int = 60):
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        return r.content if r.status_code == 200 else None
    except Exception:
        return None


def _parse_klines_csv(content: bytes, symbol: str) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(content))
    needed = ["open_time", "open", "high", "low", "close", "volume"]
    for c in needed:
        if c not in df.columns:
            return pd.DataFrame()
    df["open_time"] = pd.to_numeric(df["open_time"], errors="coerce")
    df = df.dropna(subset=["open_time"])
    df["ts"] = pd.to_datetime(df["open_time"].astype("int64"), unit="ms", utc=True)
    df = df.astype({"close": float, "open": float, "high": float, "low": float, "volume": float})
    df["symbol"] = symbol
    return df[["ts", "symbol", "open", "high", "low", "close", "volume"]]


def _parse_funding_csv(content: bytes, symbol: str) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(content))
    if "calc_time" not in df.columns or "last_funding_rate" not in df.columns:
        return pd.DataFrame()
    df["calc_time"] = pd.to_numeric(df["calc_time"], errors="coerce")
    df["last_funding_rate"] = pd.to_numeric(df["last_funding_rate"], errors="coerce")
    df = df.dropna(subset=["calc_time", "last_funding_rate"])
    if df.empty:
        return pd.DataFrame()
    df["ts"] = pd.to_datetime(df["calc_time"].astype("int64"), unit="ms", utc=True)
    df["symbol"] = symbol
    df["funding_rate"] = df["last_funding_rate"]
    return df[["ts", "symbol", "funding_rate"]]


def fetch_klines_vision(symbol: str, interval: str, start: str, end: str) -> pd.DataFrame:
    all_dfs = []
    for date_str, _ in _date_range(start, end):
        url = f"{DATA_VISION_BASE}/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{date_str}.zip"
        raw = _download_zip(url)
        if raw:
            with zipfile.ZipFile(io.BytesIO(raw), "r") as z:
                names = z.namelist()
                if names:
                    content = z.read(names[0])
                    df = _parse_klines_csv(content, symbol)
                    all_dfs.append(df)
        time.sleep(0.1)
    if not all_dfs:
        return pd.DataFrame()
    out = pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset=["ts"]).sort_values("ts")
    end_ts = pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1)
    return out[(out["ts"] >= pd.Timestamp(start, tz="UTC")) & (out["ts"] < end_ts)]


def fetch_funding_vision(symbol: str, start: str, end: str) -> pd.DataFrame:
    all_dfs = []
    months = pd.date_range(start, end, freq="MS").strftime("%Y-%m").tolist()
    for ym in months:
        url = f"{DATA_VISION_BASE}/monthly/fundingRate/{symbol}/{symbol}-fundingRate-{ym}.zip"
        raw = _download_zip(url)
        if raw:
            with zipfile.ZipFile(io.BytesIO(raw), "r") as z:
                names = z.namelist()
                if names:
                    content = z.read(names[0])
                    df = _parse_funding_csv(content, symbol)
                    if not df.empty:
                        all_dfs.append(df)
        time.sleep(0.1)
    if not all_dfs:
        return pd.DataFrame()
    out = pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset=["ts"]).sort_values("ts")
    end_ts = pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1)
    return out[(out["ts"] >= pd.Timestamp(start, tz="UTC")) & (out["ts"] < end_ts)]


def build_local_dataset(cfg: dict, data_dir: Path) -> None:
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    start = cfg.get("start", START_DATE)
    end = cfg.get("end", END_DATE)
    symbols = cfg.get("symbols", SYMBOLS)

    print("Downloading from data.binance.vision...")
    k1h_list, k5m_list, fund_list = [], [], []
    for s in symbols:
        print(f"  {s}...", end=" ", flush=True)
        k1 = fetch_klines_vision(s, "1h", start, end)
        k5 = fetch_klines_vision(s, "5m", start, end)
        fr = fetch_funding_vision(s, start, end)
        if not k1.empty:
            k1h_list.append(k1)
        if not k5.empty:
            k5m_list.append(k5)
        if not fr.empty:
            fund_list.append(fr)
        print("ok" if not k1.empty else "empty", flush=True)
        time.sleep(0.2)

    k1h = pd.concat(k1h_list, ignore_index=True) if k1h_list else pd.DataFrame()
    k5m = pd.concat(k5m_list, ignore_index=True) if k5m_list else pd.DataFrame()
    fund = pd.concat(fund_list, ignore_index=True) if fund_list else pd.DataFrame()

    if not k1h.empty:
        k1h.to_parquet(data_dir / "klines_1h.parquet", index=False)
        print("Saved klines_1h.parquet")
    if not k5m.empty:
        k5m.to_parquet(data_dir / "klines_5m.parquet", index=False)
        print("Saved klines_5m.parquet")
    if not fund.empty:
        fund.to_parquet(data_dir / "funding_8h.parquet", index=False)
        print("Saved funding_8h.parquet")

    print("(OI not available -> align.py uses volume_change as crowding proxy)")
    print("Done. Files in", data_dir)
