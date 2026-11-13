# Streaming Pipeline - Real-Time Event Processing

A real-time data streaming simulation that processes e-commerce events. This project demonstrates how the producer-consumer pattern and threading can be used to handle streaming data.

## What Does This Project Do?

This project simulates:
1. An **online store** continuously generating transaction events (`order_placed`, `payment_confirmed`, `order_shipped`, and `order_delivered`)
2. An **analytics system** aggregating data in real time
3. **Anomaly detection** for cities with unusually high revenue
4. A **simple dashboard** displaying analysis results

## Getting Started

### 1. Verify Python Installation

Check that Python 3.8 or newer is installed:

```bash
python --version
```

### 2. Clone or Download the Project

```bash
git clone <repo-url>
cd stream_pipeline
```

### 3. Optional: Install Enhanced Chart Support

```bash
pip install plotext
```

Without this dependency, the application still works using text-based visualizations.

### 4. Run the Application

```bash
python stream_pipeline.py
```

By default, the application runs for 30 seconds and will:
- Generate 20 events per second
- Aggregate data every 3 seconds
- Display a final analytics summary

## Configuration

Open `stream_pipeline.py` and locate the `Config` class near the top of the file:

```python
class Config:
    DEFAULT_DURATION = 30              # Runtime in seconds
    DEFAULT_EVENTS_PER_SEC = 20        # Events generated per second
    WINDOW_SECONDS = 3                 # Aggregation window in seconds
    ANOMALY_THRESHOLD = 300000         # Revenue anomaly threshold (IDR 300,000)
```

Change the values as needed, save the file, and run the application again.

## Output

### 1. Real-Time Logs

```text
2026-11-02 12:00:01 - INFO - Producer started: 20 events/sec for 30s
2026-11-02 12:00:04 - INFO - Window: 60 events (20.0 events/sec) | Total revenue: Rp15.234.567
```

### 2. Summary Metrics

- Total events processed
- Total revenue in Indonesian Rupiah
- Average events processed per second
- City with the highest revenue
- Most frequent event type

### 3. Event Distribution

A breakdown of each event type, including `order_placed` and `payment_confirmed`, displayed as a simple bar chart.

### 4. Revenue by City

Revenue details for Jakarta, Bandung, Surabaya, Medan, and Bali, including the average order value.

### 5. Anomaly Detection

A list of cities whose total revenue exceeds the configured anomaly threshold.

## Architecture

```text
Producer Thread                Consumer Thread
     │                              │
     ├─ Generate event ────────────>│
     │  (order information)         │
     │                              ├─ Aggregate
     │                              │  (total, average, etc.)
     │                              │
     └─ [Queue: 10k capacity] ─────>└─ Store in state
                                          │
                                          └─ Analytics and reporting
```

- **Producer:** Continuously generates events
- **Queue:** Temporarily stores events in a thread-safe structure
- **Consumer:** Retrieves and processes events from the queue
- **State:** Stores aggregation results

## Generated Data

### Event Types

- `order_placed` - A new order is created
- `payment_confirmed` - Payment is completed
- `order_shipped` - The order is shipped
- `order_delivered` - The order is delivered

### Cities

Jakarta, Bandung, Surabaya, Medan, and Bali.

### Order Value

Random values between IDR 10,000 and IDR 500,000.

## Troubleshooting

### The application crashes or reports an error

- Verify that Python 3.8 or newer is installed
- Restart the application

### Stop the application before completion

Press `Ctrl+C`.

### Generate more or fewer events

Change `DEFAULT_DURATION` and `DEFAULT_EVENTS_PER_SEC` in the `Config` class.

## Use Cases

This project is suitable for:
- Learning stream-processing fundamentals
- Demonstrating the producer-consumer pattern
- Practicing multithreading in Python
- Serving as a foundation for Kafka, Spark Streaming, or similar systems
