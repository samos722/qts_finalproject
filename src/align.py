# Aligns 1h klines + funding, computes ret_1h & volume_change, outputs panel_1h.parquet
"""
Align data to 1h grid, merge funding, build panel.
Crowding proxy: volume change (OI not available on data.binance.vision).
"""

from pathlib import Path

import pandas as pd

from .config import ALPHA_HOURS, VOL_CHANGE_WINDOW_HOURS


def align_to_panel(data_dir: Path, vol_change_window: int = None, alpha_hours: list = None) -> pd.DataFrame:
    alpha_hours = alpha_hours or ALPHA_HOURS
    vol_change_window = vol_change_window or VOL_CHANGE_WINDOW_HOURS

    k1h = pd.read_parquet(data_dir / "klines_1h.parquet")
    fund = pd.read_parquet(data_dir / "funding_8h.parquet")

    k1h["ts"] = pd.to_datetime(k1h["ts"]).dt.tz_localize(None).dt.tz_localize("UTC", ambiguous="infer")
    fund["ts"] = pd.to_datetime(fund["ts"]).dt.tz_localize(None).dt.tz_localize("UTC", ambiguous="infer")

    k1h = k1h.sort_values(["symbol", "ts"])
    k1h["ret_1h"] = k1h.groupby("symbol")["close"].pct_change()
    # Crowding proxy: volume change over N hours (替代 OI)
    k1h["volume_change"] = k1h.groupby("symbol")["volume"].pct_change(periods=vol_change_window)

    fund_merge = fund.copy()
    fund_merge["ts_hour"] = fund_merge["ts"].dt.floor("h")
    fund_merge = fund_merge[["symbol", "ts_hour", "funding_rate"]].drop_duplicates(subset=["symbol", "ts_hour"])
    k1h = k1h.copy()
    k1h["ts_hour"] = k1h["ts"]
    merged = k1h.merge(fund_merge, on=["symbol", "ts_hour"], how="left")
    merged = merged.drop(columns=["ts_hour"], errors="ignore")

    merged["ts_hour"] = merged["ts"].dt.floor("h")
    merged["hour_utc"] = merged["ts_hour"].dt.hour
    merged["is_alpha_time"] = merged["hour_utc"].isin(alpha_hours)
    merged = merged.drop(columns=["ts_hour", "hour_utc"], errors="ignore")

    out_cols = ["ts", "symbol", "close", "ret_1h", "funding_rate", "volume_change", "is_alpha_time"]
    for c in ["open", "high", "low", "volume"]:
        if c in merged.columns:
            out_cols.append(c)
    out_cols = [c for c in out_cols if c in merged.columns]
    panel = merged[out_cols].drop_duplicates(subset=["ts", "symbol"]).sort_values(["ts", "symbol"])
    return panel
