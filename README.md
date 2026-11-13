# Streaming Pipeline - Real-Time Event Processing

A Python producer-consumer simulation for e-commerce event analytics. It uses threads and a bounded queue to generate, drain, aggregate, and report synthetic events.

## What It Does

1. Generates independent synthetic events: `order_placed`, `payment_confirmed`, `order_shipped`, and `order_delivered`
2. Drains every produced event before normal shutdown
3. Counts events by type and city
4. Counts revenue only from `payment_confirmed` events
5. Evaluates city revenue alerts separately for each processing-time window
6. Displays a final text-only terminal dashboard

Events are independent samples, not stages in an order lifecycle. A payment event is not guaranteed to match a preceding order event.

## Getting Started

Python 3.8 or newer is required. No external dependencies are required.

```bash
git clone https://github.com/sultanmrgnc/real-time-analytics-pipeline.git
cd real-time-analytics-pipeline
python stream_pipeline.py
```

The default run lasts 30 seconds, produces 20 events per second, and uses 3-second processing-time windows. A final partial window is flushed after the producer finishes and the queue drains. Press `Ctrl+C` to stop production; queued events are still drained before the report.

## Configuration

Update `Config` in `stream_pipeline.py`:

```python
class Config:
    DEFAULT_DURATION = 30
    DEFAULT_EVENTS_PER_SEC = 20
    WINDOW_SECONDS = 3
    ANOMALY_THRESHOLD = 300000
```

Duration, event rate, and window duration must be positive.

## Output

The terminal output includes:

- Total processed events
- Total confirmed-payment revenue in Indonesian Rupiah
- Average events per second
- Average confirmed-payment value
- Revenue by city and confirmed-payment count
- Event-type distribution
- Per-window revenue threshold alerts

An alert is emitted when a city's confirmed-payment revenue in one processing-time window exceeds `ANOMALY_THRESHOLD`. It is a rule-based threshold alert, not statistical anomaly detection. City revenue and average payment values use only confirmed-payment events.

## Architecture

```text
Producer Thread                 Consumer Thread
     │                                │
     ├─ Generate synthetic event ────>│
     │                                ├─ Aggregate and flush windows
     └─ [Bounded Queue] ─────────────>└─ Final terminal report
```

- **Producer:** Generates independent synthetic events until duration expires or shutdown is requested.
- **Queue:** Thread-safe bounded event buffer.
- **Consumer:** Processes events, flushes elapsed windows even when the queue is empty, then flushes the final partial window after draining.
- **State:** Stores event counts, payment revenue, window throughput, and alerts.

## Generated Data

Cities: Jakarta, Bandung, Surabaya, Medan, and Bali.

Each event has a random value between IDR 10,000 and IDR 500,000. Only `payment_confirmed` values contribute to revenue.
