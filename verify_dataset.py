#!/usr/bin/env python3
"""Kiểm tra chất lượng dữ liệu trong data/parquet: khoảng trống, trùng lặp, OHLC sai, aggTrades liên tục."""
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

P = Path("data/parquet")


def gaps(idx, freq, top=5):
    d = idx.to_series().diff()
    g = d[d > pd.Timedelta(freq)].sort_values(ascending=False)
    return g, g.head(top)


print("=== Klines / mark / index / premium (1m)")
for name in ["klines_1m", "markPrice_1m", "indexPrice_1m", "premiumIndex_1m"]:
    df = pd.read_parquet(P / f"{name}.parquet")
    g, top = gaps(df.index, "1min")
    missing = int((g / pd.Timedelta("1min") - 1).sum())
    bad = ((df.high < df[["open", "close"]].max(axis=1)) | (df.low > df[["open", "close"]].min(axis=1))).sum()
    print(f"{name:16s} {len(df):>10,} rows  {df.index.min():%Y-%m-%d %H:%M} → {df.index.max():%Y-%m-%d %H:%M}"
          f"  dup={df.index.duplicated().sum()}  gaps={len(g)} (thiếu {missing:,} phút)  OHLC lỗi={bad}")
    for t, d in top.items():
        print(f"    gap {d} kết thúc lúc {t}")

fr = pd.read_parquet(P / "fundingRate.parquet")
print(f"\n=== fundingRate {len(fr):,} kỳ  {fr.index.min()} → {fr.index.max()}")
mt = pd.read_parquet(P / "metrics_5m.parquet")
g, _ = gaps(mt.index, "5min")
print(f"=== metrics_5m {len(mt):,} rows  {mt.index.min()} → {mt.index.max()}  gaps={len(g)}  NaN%={mt.isna().mean().mean():.2%}")
bd = pq.ParquetFile(P / "bookDepth.parquet").metadata.num_rows
print(f"=== bookDepth {bd:,} rows")

print("\n=== aggTrades (kiểm tra agg_trade_id liên tục giữa các file)")
total, prev_last, breaks = 0, None, []
for f in sorted((P / "aggTrades").glob("*.parquet")):
    t = pq.read_table(f, columns=["agg_trade_id"]).column(0)
    first, last = t[0].as_py(), t[-1].as_py()
    n = len(t)
    total += n
    if prev_last is not None and first != prev_last + 1:
        breaks.append((f.stem, prev_last, first))
    internal = (last - first + 1) - n
    if internal:
        breaks.append((f.stem + " (nội bộ)", n, last - first + 1))
    prev_last = last
print(f"files={len(list((P / 'aggTrades').glob('*.parquet')))}  trades={total:,}  breaks={len(breaks)}")
for b in breaks[:15]:
    print("   ", b)
