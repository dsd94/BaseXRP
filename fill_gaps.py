#!/usr/bin/env python3
"""
Vá các ngày bị thiếu trong file monthly của data.binance.vision.

- Klines/mark/index/premium 1m: lấy phút còn thiếu qua REST API fapi (có lịch sử đầy đủ).
- aggTrades: tìm đoạn agg_trade_id bị hở, tải file *daily* của các ngày đó rồi gộp vào file tháng.

Được gọi từ build_dataset.py; có thể chạy riêng: .venv/bin/python fill_gaps.py
"""
import json
import ssl
import time
import urllib.request
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

try:
    import certifi
    CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    CTX = ssl.create_default_context()

FAPI = "https://fapi.binance.com/fapi/v1/"
ENDPOINT = {
    "klines": "klines?symbol={s}",
    "markPriceKlines": "markPriceKlines?symbol={s}",
    "indexPriceKlines": "indexPriceKlines?pair={s}",
    "premiumIndexKlines": "premiumIndexKlines?symbol={s}",
}
KCOLS = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume",
         "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore"]


def get_json(url, retries=5):
    for i in range(retries):
        try:
            return json.load(urllib.request.urlopen(url, timeout=30, context=CTX))
        except Exception:  # noqa: BLE001
            if i == retries - 1:
                raise
            time.sleep(2 ** i)


def missing_ranges(idx, freq="1min"):
    d = idx.to_series().diff()
    g = d[d > pd.Timedelta(freq)]
    return [(t - dt + pd.Timedelta(freq), t - pd.Timedelta(freq)) for t, dt in g.items()]


def fill_klines(df, dataset, symbol="XRPUSDT"):
    """df: index open_time (UTC), cột như build_dataset. Trả về df đã vá + số phút vá được."""
    ranges = missing_ranges(df.index)
    rows = []
    for start, end in ranges:
        cur, stop = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
        while cur <= stop:
            url = f"{FAPI}{ENDPOINT[dataset].format(s=symbol)}&interval=1m&startTime={cur}&endTime={stop}&limit=1500"
            data = get_json(url)
            if not data:
                break
            rows += data
            cur = data[-1][0] + 60_000
            time.sleep(0.1)
    if not rows:
        return df, 0
    add = pd.DataFrame(rows, columns=KCOLS)
    add["open_time"] = pd.to_datetime(add["open_time"], unit="ms", utc=True)
    add = add.set_index("open_time")[[c for c in df.columns]].astype(df.dtypes.to_dict())
    out = pd.concat([df, add])
    out = out[~out.index.duplicated()].sort_index()
    return out, len(out) - len(df)


def fill_aggtrades(out_dir=Path("data/parquet/aggTrades"), raw=Path("data/raw"), symbol="XRPUSDT"):
    """Vá các khoảng hở agg_trade_id bằng file daily. Trả về số lệnh đã thêm."""
    from download_binance import fetch
    from build_dataset import COLS, read_zip_csv, to_dt

    files = sorted(out_dir.glob("*.parquet"))
    added_total, prev = 0, None   # prev = (last_id, last_time) của file trước
    for f in files:
        t = pq.read_table(f, columns=["agg_trade_id", "transact_time"]).to_pandas()
        ids = t["agg_trade_id"]
        need_days = set()
        if prev is not None and ids.iloc[0] != prev[0] + 1:
            need_days.update(pd.date_range(prev[1].normalize(), t["transact_time"].iloc[0].normalize()))
        dup = ids.duplicated().any()
        s = t.sort_values("agg_trade_id").drop_duplicates("agg_trade_id")
        jumps = s["agg_trade_id"].diff() > 1
        for i in s.index[jumps]:
            pos = s.index.get_loc(i)
            a, b = s["transact_time"].iloc[pos - 1], s["transact_time"].iloc[pos]
            need_days.update(pd.date_range(a.normalize(), b.normalize()))
        prev = (int(s["agg_trade_id"].iloc[-1]), s["transact_time"].iloc[-1])
        if not need_days and not dup:
            continue
        month = f.stem[:7]
        extra = []
        for day in sorted(need_days):
            key = f"data/futures/um/daily/aggTrades/{symbol}/{symbol}-aggTrades-{day:%Y-%m-%d}.zip"
            try:
                fetch(key, raw)
            except Exception as e:  # noqa: BLE001
                print(f"    không tải được {key}: {e}")
                continue
            d = read_zip_csv(raw / key.removeprefix("data/"), COLS["aggTrades"])
            d["transact_time"] = to_dt(d["transact_time"])
            d["is_buyer_maker"] = d["is_buyer_maker"].astype(str).str.lower().eq("true")
            extra.append(d)
        full = pd.read_parquet(f)
        before = len(full)
        if extra:
            full = pd.concat([full] + extra, ignore_index=True)
        # chỉ giữ lệnh thuộc đúng file này (tháng hoặc ngày) để không chồng chéo file kế tiếp
        tm = full["transact_time"].dt.strftime("%Y-%m-%d" if len(f.stem) > 7 else "%Y-%m")
        full = full[tm == f.stem]
        full = full.drop_duplicates("agg_trade_id").sort_values("agg_trade_id")
        full = full.astype({"agg_trade_id": "int64", "price": "float64", "quantity": "float64",
                            "first_trade_id": "int64", "last_trade_id": "int64"})
        tmp = f.with_suffix(".parquet.part")
        full.to_parquet(tmp, index=False, compression="zstd")
        tmp.replace(f)
        added_total += len(full) - before
        print(f"    {f.stem}: {len(need_days)} ngày daily, {len(full) - before:+,} lệnh"
              + (" (đã bỏ trùng)" if dup else ""), flush=True)
        prev = (int(full["agg_trade_id"].iloc[-1]), full["transact_time"].iloc[-1])
    return added_total


if __name__ == "__main__":
    print("aggTrades:", fill_aggtrades())
