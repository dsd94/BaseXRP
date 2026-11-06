# BaseXRP

Bot giao dịch XRP Futures (Binance USDⓈ-M, XRPUSDT). Giai đoạn hiện tại: pipeline dữ liệu lịch sử.

## Cài đặt

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Dữ liệu

```bash
.venv/bin/python download_binance.py   # tải zip từ data.binance.vision (~12 GB), kiểm tra SHA256, chạy lại được
.venv/bin/python build_dataset.py      # gộp sang Parquet trong data/parquet, vá ngày thiếu
.venv/bin/python verify_dataset.py     # kiểm tra khoảng trống / trùng lặp
```

| File (data/parquet) | Nội dung | Từ |
|---|---|---|
| `klines_1m` (+ `5m/15m/1h/4h/1d`) | OHLCV, số lệnh, taker buy | 2020-01 |
| `markPrice_1m`, `indexPrice_1m`, `premiumIndex_1m` | Giá mark / index / premium | 2020-01 |
| `fundingRate` | Funding mỗi kỳ (cập nhật tới hiện tại qua API) | 2020-01 |
| `metrics_5m` | Open interest, long/short ratio, taker ratio | 2021-12 |
| `bookDepth` | Độ sâu sổ lệnh ±1–5% | 2023-01 |
| `aggTrades/YYYY-MM` | Tick aggTrades (~790 triệu lệnh) | 2020-01 |
| `XRPUSDT_1m_features` | Bảng gộp 1m: OHLCV + mark/index/premium + funding + metrics | 2020-01 |

`download_binance.py --datasets all` tải thêm `trades` thô (~36 GB) và `bookTicker`.
