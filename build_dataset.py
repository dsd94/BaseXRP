#!/usr/bin/env python3
"""
Gộp các file zip từ data/raw (do download_binance.py tải) thành Parquet sạch trong data/parquet.

Đầu ra:
  klines_1m.parquet            OHLCV giao dịch 1 phút (+ taker buy, số lệnh)
  markPrice_1m / indexPrice_1m / premiumIndex_1m .parquet
  fundingRate.parquet          funding mỗi kỳ (8h, có thể đổi interval)
  metrics_5m.parquet           Open interest, long/short ratio (top trader, toàn bộ), taker ratio
  bookDepth.parquet            độ sâu sổ lệnh theo % (±1..5%)
  aggTrades/YYYY-MM.parquet    tick aggTrades theo tháng
  XRPUSDT_1m_features.parquet  bảng gộp 1m: OHLCV + mark/index/premium + funding + metrics (ffill)
  klines_{5m,15m,1h,4h,1d}.parquet  resample từ 1m

    .venv/bin/python build_dataset.py [--skip-aggtrades]
"""
import argparse
import io
import zipfile
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pandas as pd

from fill_gaps import fill_aggtrades, fill_klines

RAW = Path("data/raw/futures/um")
OUT = Path("data/parquet")

KLINE_COLS = ["open_time", "open", "high", "low", "close", "volume", "close_time",
              "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore"]
COLS = {
    "fundingRate": ["calc_time", "funding_interval_hours", "last_funding_rate"],
    "metrics": ["create_time", "symbol", "sum_open_interest", "sum_open_interest_value",
                "count_toptrader_long_short_ratio", "sum_toptrader_long_short_ratio",
                "count_long_short_ratio", "sum_taker_long_short_vol_ratio"],
    "bookDepth": ["timestamp", "percentage", "depth", "notional"],
    "aggTrades": ["agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id",
                  "transact_time", "is_buyer_maker"],
}


def read_zip_csv(path, cols):
    with zipfile.ZipFile(path) as z:
        raw = z.read(z.namelist()[0])
    first = raw.split(b"\n", 1)[0].split(b",")[0].strip()
    has_header = not first.replace(b".", b"").lstrip(b"-").isdigit()
    return pd.read_csv(io.BytesIO(raw), header=0 if has_header else None,
                       names=cols, low_memory=False)


def to_dt(s):
    s = pd.to_numeric(s, errors="coerce")
    unit = "us" if s.dropna().gt(1e14).any() else "ms"   # Binance có thể chuyển sang microsecond
    return pd.to_datetime(s, unit=unit, utc=True)


def files(dataset, sub=""):
    fs = sorted((RAW / "monthly" / dataset / "XRPUSDT" / sub).glob("*.zip")) + \
         sorted((RAW / "daily" / dataset / "XRPUSDT" / sub).glob("*.zip"))
    return fs


def load_all(dataset, cols, sub=""):
    fs = files(dataset, sub)
    if not fs:
        return None
    return pd.concat([read_zip_csv(f, cols) for f in fs], ignore_index=True)


def build_klines(dataset, name):
    df = load_all(dataset, KLINE_COLS, "1m")
    if df is None:
        print(f"  (bỏ qua {name}: chưa có dữ liệu)")
        return None
    df = df.drop(columns=["ignore", "close_time"])
    df["open_time"] = to_dt(df["open_time"])
    df = df.drop_duplicates("open_time").sort_values("open_time").set_index("open_time")
    df = df.astype({c: "float64" for c in df.columns if c != "count"})
    if dataset != "klines":   # mark/index/premium: chỉ có OHLC giá có ý nghĩa
        df = df[["open", "high", "low", "close"]]
    df, n = fill_klines(df, dataset)
    if n:
        print(f"  + vá {n:,} phút thiếu qua REST API")
    df.to_parquet(OUT / f"{name}.parquet")
    print(f"  {name}: {len(df):,} rows  {df.index.min()} → {df.index.max()}")
    return df


def agg_one(path):
    """Chuyển 1 file aggTrades sang parquet theo từng khúc (ít RAM), ghi nguyên tử, bỏ qua nếu đã có."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    tag = path.stem.replace("XRPUSDT-aggTrades-", "")
    out = OUT / "aggTrades" / f"{tag}.parquet"
    if out.exists():
        try:
            return tag, pq.ParquetFile(out).metadata.num_rows
        except Exception:  # noqa: BLE001
            out.unlink()
    tmp = out.with_suffix(".parquet.part")
    n, writer = 0, None
    with zipfile.ZipFile(path) as z, z.open(z.namelist()[0]) as fh:
        first = fh.peek(64)[:64].split(b",")[0].strip()
        has_header = not first.replace(b".", b"").isdigit()
        for df in pd.read_csv(fh, header=0 if has_header else None, names=COLS["aggTrades"],
                              chunksize=5_000_000):
            df["transact_time"] = to_dt(df["transact_time"])
            df["is_buyer_maker"] = df["is_buyer_maker"].astype(str).str.lower().eq("true")
            df = df.astype({"agg_trade_id": "int64", "price": "float64", "quantity": "float64",
                            "first_trade_id": "int64", "last_trade_id": "int64"})
            t = pa.Table.from_pandas(df, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(tmp, t.schema, compression="zstd")
            writer.write_table(t)
            n += len(df)
    writer.close()
    tmp.replace(out)
    return tag, n


def topup_funding(fr):
    """Bổ sung funding rate gần nhất (tháng hiện tại chưa có file monthly) qua REST API."""
    import json
    import ssl
    import urllib.request
    try:
        import certifi
        ctx = ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        ctx = ssl.create_default_context()
    start = int(fr.index.max().timestamp() * 1000) + 1
    rows = []
    while True:
        url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol=XRPUSDT&startTime={start}&limit=1000"
        try:
            data = json.load(urllib.request.urlopen(url, timeout=30, context=ctx))
        except Exception as e:  # noqa: BLE001
            print(f"  (không gọi được API funding: {e})")
            break
        if not data:
            break
        rows += data
        start = data[-1]["fundingTime"] + 1
        if len(data) < 1000:
            break
    if not rows:
        return fr
    api = pd.DataFrame({"calc_time": pd.to_datetime([r["fundingTime"] for r in rows], unit="ms", utc=True).floor("min"),
                        "funding_interval_hours": pd.NA,
                        "last_funding_rate": [float(r["fundingRate"]) for r in rows]}).set_index("calc_time")
    print(f"  + {len(api)} kỳ funding từ API")
    out = pd.concat([fr, api])
    return out[~out.index.duplicated()].sort_index()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-aggtrades", action="store_true")
    ap.add_argument("--workers", type=int, default=2, help="số tiến trình convert aggTrades")
    a = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)

    print("Klines…")
    k = build_klines("klines", "klines_1m")
    mark = build_klines("markPriceKlines", "markPrice_1m")
    idx = build_klines("indexPriceKlines", "indexPrice_1m")
    prem = build_klines("premiumIndexKlines", "premiumIndex_1m")

    print("Funding rate…")
    fr = load_all("fundingRate", COLS["fundingRate"])
    fr["calc_time"] = to_dt(fr["calc_time"]).dt.floor("min")
    fr = fr.drop_duplicates("calc_time").sort_values("calc_time").set_index("calc_time")
    fr = topup_funding(fr)
    fr.to_parquet(OUT / "fundingRate.parquet")
    print(f"  fundingRate: {len(fr):,} rows  {fr.index.min()} → {fr.index.max()}")

    print("Metrics…")
    mt = load_all("metrics", COLS["metrics"])
    if mt is not None:
        mt["create_time"] = pd.to_datetime(mt["create_time"], utc=True, errors="coerce")
        mt = mt.drop(columns=["symbol"]).dropna(subset=["create_time"])
        mt = mt.drop_duplicates("create_time").sort_values("create_time").set_index("create_time")
        mt = mt.apply(pd.to_numeric, errors="coerce")
        mt.to_parquet(OUT / "metrics_5m.parquet")
        print(f"  metrics: {len(mt):,} rows  {mt.index.min()} → {mt.index.max()}")

    print("Book depth…")
    bd = load_all("bookDepth", COLS["bookDepth"])
    if bd is not None:
        bd["timestamp"] = pd.to_datetime(bd["timestamp"], utc=True, errors="coerce")
        bd = bd.dropna(subset=["timestamp"]).sort_values(["timestamp", "percentage"])
        bd.to_parquet(OUT / "bookDepth.parquet", index=False)
        print(f"  bookDepth: {len(bd):,} rows  {bd.timestamp.min()} → {bd.timestamp.max()}")

    print("Feature table 1m…")
    feat = k.copy()
    for name, d in (("mark", mark), ("index", idx), ("premium", prem)):
        if d is not None:
            feat[f"{name}_close"] = d["close"].reindex(feat.index)
    feat["funding_rate"] = fr["last_funding_rate"].astype(float).reindex(feat.index, method="ffill")
    if mt is not None:
        m1 = mt.reindex(feat.index, method="ffill", tolerance=pd.Timedelta("10min"))
        feat = feat.join(m1)
    feat.to_parquet(OUT / "XRPUSDT_1m_features.parquet")
    print(f"  features: {feat.shape}")

    print("Resample…")
    agg = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum",
           "quote_volume": "sum", "count": "sum", "taker_buy_volume": "sum",
           "taker_buy_quote_volume": "sum"}
    for rule, label in (("5min", "5m"), ("15min", "15m"), ("1h", "1h"), ("4h", "4h"), ("1D", "1d")):
        r = k.resample(rule).agg(agg).dropna(subset=["open"])
        r.to_parquet(OUT / f"klines_{label}.parquet")
        print(f"  klines_{label}: {len(r):,} rows")

    if not a.skip_aggtrades:
        print("aggTrades → parquet theo tháng…")
        (OUT / "aggTrades").mkdir(exist_ok=True)
        fs = files("aggTrades")
        total = 0
        with ProcessPoolExecutor(a.workers) as ex:
            for tag, n in ex.map(agg_one, fs):
                total += n
                print(f"    {tag}: {n:,}", flush=True)
        print(f"  aggTrades: {len(fs)} files, {total:,} trades")
        print("  vá khoảng hở aggTrades bằng file daily…")
        print(f"  + {fill_aggtrades():,} lệnh")
    print("DONE")


if __name__ == "__main__":
    main()
