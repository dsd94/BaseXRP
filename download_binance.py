#!/usr/bin/env python3
"""
Tải dữ liệu lịch sử Binance USDⓈ-M Futures (mặc định XRPUSDT) từ data.binance.vision.

- Liệt kê file qua S3 listing (có phân trang), tải song song, kiểm tra SHA256 (.CHECKSUM).
- Dùng file monthly; với tháng chưa có file monthly (tháng hiện tại) thì lấy file daily.
- Chạy lại an toàn: file đã tải và đúng checksum sẽ được bỏ qua (resume).

Ví dụ:
    .venv/bin/python download_binance.py                       # bộ dữ liệu mặc định
    .venv/bin/python download_binance.py --datasets aggTrades  # chỉ aggTrades (~12 GB)
    .venv/bin/python download_binance.py --datasets all        # gồm cả trades thô (~36 GB) + bookTicker
"""
import argparse
import hashlib
import re
import ssl
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

LIST_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
DL_URL = "https://data.binance.vision/"
try:
    import certifi
    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()

NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

# dataset -> (có thư mục interval?, có monthly?)
DATASETS = {
    "klines":             (True,  True),
    "markPriceKlines":    (True,  True),
    "indexPriceKlines":   (True,  True),
    "premiumIndexKlines": (True,  True),
    "fundingRate":        (False, True),   # chỉ có monthly
    "metrics":            (False, False),  # chỉ có daily: OI, long/short ratio, taker ratio (5m)
    "bookDepth":          (False, False),  # chỉ có daily: độ sâu sổ lệnh theo % giá
    "aggTrades":          (False, True),   # ~12 GB zip
    "bookTicker":         (False, True),   # L1 tick 2023-05 → 2024-03, rất nặng
    "trades":             (False, True),   # ~36 GB zip
}
DEFAULT = ["klines", "markPriceKlines", "indexPriceKlines", "premiumIndexKlines",
           "fundingRate", "metrics", "bookDepth", "aggTrades"]


def http_get(url, retries=5, timeout=120):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "xrpbot-downloader/1.0"})
            with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
                return r.read()
        except Exception as e:  # noqa: BLE001
            if i == retries - 1:
                raise
            time.sleep(2 ** i)


def list_keys(prefix):
    """Trả về list các key .zip dưới prefix (xử lý phân trang S3)."""
    keys, marker = [], ""
    while True:
        url = f"{LIST_URL}?prefix={prefix}" + (f"&marker={marker}" if marker else "")
        root = ET.fromstring(http_get(url))
        batch = [c.find("s3:Key", NS).text for c in root.findall("s3:Contents", NS)]
        keys += [k for k in batch if k.endswith(".zip")]
        truncated = root.find("s3:IsTruncated", NS).text == "true"
        if not truncated or not batch:
            return keys
        marker = batch[-1]


def plan(symbol, dataset, interval, market):
    has_iv, has_monthly = DATASETS[dataset]
    sub = f"{symbol}/{interval}/" if has_iv else f"{symbol}/"
    monthly = list_keys(f"data/futures/{market}/monthly/{dataset}/{sub}") if has_monthly else []
    daily = list_keys(f"data/futures/{market}/daily/{dataset}/{sub}")
    covered = set()
    for k in monthly:
        m = re.search(r"(\d{4}-\d{2})\.zip$", k)
        if m:
            covered.add(m.group(1))
    # chỉ lấy daily cho các tháng chưa có file monthly
    daily = [k for k in daily if (m := re.search(r"(\d{4}-\d{2})-\d{2}\.zip$", k)) and m.group(1) not in covered]
    return monthly + daily


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch(key, out_root):
    dest = out_root / key.removeprefix("data/")
    ok_marker = dest.with_suffix(".zip.ok")
    if dest.exists() and ok_marker.exists():
        return key, "skip", 0
    dest.parent.mkdir(parents=True, exist_ok=True)
    expected = None
    try:
        expected = http_get(DL_URL + key + ".CHECKSUM").decode().split()[0]
    except Exception:  # noqa: BLE001
        pass
    for attempt in range(3):
        tmp = dest.with_suffix(".zip.part")
        tmp.write_bytes(http_get(DL_URL + key, timeout=600))
        if expected is None or sha256(tmp) == expected:
            tmp.replace(dest)
            ok_marker.write_text(expected or "nochecksum")
            return key, "ok" if expected else "ok-nochecksum", dest.stat().st_size
    raise RuntimeError(f"checksum mismatch: {key}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="XRPUSDT")
    ap.add_argument("--market", default="um", help="um (USDⓈ-M) hoặc cm (COIN-M)")
    ap.add_argument("--datasets", nargs="+", default=DEFAULT, help="tên dataset hoặc 'all'")
    ap.add_argument("--interval", default="1m")
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--workers", type=int, default=12)
    a = ap.parse_args()
    datasets = list(DATASETS) if a.datasets == ["all"] else a.datasets
    out_root = Path(a.out)

    for ds in datasets:
        keys = plan(a.symbol, ds, a.interval, a.market)
        print(f"[{ds}] {len(keys)} files", flush=True)
        done, total_bytes, errors = 0, 0, []
        with ThreadPoolExecutor(a.workers) as ex:
            futs = {ex.submit(fetch, k, out_root): k for k in keys}
            for f in as_completed(futs):
                done += 1
                try:
                    _, st, n = f.result()
                    total_bytes += n
                except Exception as e:  # noqa: BLE001
                    errors.append((futs[f], str(e)))
                if done % 50 == 0 or done == len(keys):
                    print(f"  {ds}: {done}/{len(keys)}  +{total_bytes/1e6:.1f} MB", flush=True)
        for k, e in errors:
            print(f"  ERROR {k}: {e}", file=sys.stderr)
    print("DONE")


if __name__ == "__main__":
    main()
