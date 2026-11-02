# Streaming Pipeline - Real-time Event Processing

Real-time event aggregation system untuk memproses dan menganalisis data streaming dengan producer-consumer pattern menggunakan threading.

## Features

- **Producer**: Generate event streaming kontinyu (order_placed, payment_confirmed, order_shipped, order_delivered)
- **Consumer**: Aggregate events dalam time windows
- **Real-time Analytics**: Total revenue, events per second, distribution by city & event type
- **Anomaly Detection**: Identifikasi kota dengan total revenue melebihi threshold
- **Visualisasi**: ASCII charts dan text-based reporting (dengan opsi plotext untuk grafik lebih baik)

## Architecture

```
Producer Thread → Event Queue (10k size) → Consumer Thread
                                              ↓
                                    Aggregation State
                                              ↓
                        Analytics & Anomaly Detection
```

## Requirements

```
Python 3.8+
plotext (optional, untuk visualisasi grafik)
```

## Installation

```bash
pip install plotext  # optional
```

## Usage

```bash
python stream_pipeline.py
```

### Custom Configuration

Edit class `Config` di `stream_pipeline.py`:

```python
class Config:
    QUEUE_SIZE = 10000
    QUEUE_TIMEOUT = 0.1
    DEFAULT_DURATION = 30              # durasi pipeline (detik)
    DEFAULT_EVENTS_PER_SEC = 20        # event generation rate
    WINDOW_SECONDS = 3                 # agregasi window size
    ANOMALY_THRESHOLD = 300000         # threshold revenue anomali (Rp)
```

## Output

### Summary Metrics
- Total events processed
- Total revenue (format Rupiah)
- Average events/second
- Top city by revenue
- Most frequent event type

### Event Distribution
Breakdown by event type dengan percentage & visual bar chart

### Revenue by City
Detail breakdown revenue per kota dengan average order value

### Anomaly Detection
Identifikasi kota yang melampaui threshold revenue

## Example Run

```
2026-11-02 05:43:01,027 - INFO - Starting streaming pipeline...
2026-11-02 05:43:01,028 - INFO - Producer started: 20 events/sec for 30s
2026-11-02 05:43:01,029 - INFO - Consumer started: aggregating every 3s window
2026-11-02 05:43:04,030 - INFO - Window: 60 events (20.0 events/sec) | Total revenue: Rp1.200.000.000
...
2026-11-02 05:43:31,050 - INFO - SUMMARY METRICS
2026-11-02 05:43:31,050 - INFO - Total Events Processed : 600
2026-11-02 05:43:31,050 - INFO - Total Revenue          : Rp10.500.000.000
```

## Cities & Event Types

**Cities**: Jakarta, Bandung, Surabaya, Medan, Bali

**Event Types**: order_placed, payment_confirmed, order_shipped, order_delivered

## Thread Safety

- Event queue menggunakan `Queue` (thread-safe)
- State updates tanpa explicit locking (single consumer pattern)
- Graceful shutdown dengan `Event` flag

## Logging

Semua aktivitas dicatat via Python logging dengan format:
```
%(asctime)s - %(levelname)s - %(message)s
```

Level: INFO, WARNING, ERROR
