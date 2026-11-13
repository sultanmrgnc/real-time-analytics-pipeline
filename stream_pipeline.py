import time
import random
import logging
from math import isfinite
from datetime import datetime
from queue import Queue, Empty
from threading import Thread, Event
from collections import defaultdict
from typing import Dict, List, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EVENT_TYPES = ['order_placed', 'payment_confirmed', 'order_shipped', 'order_delivered']
CITIES = ['Jakarta', 'Bandung', 'Surabaya', 'Medan', 'Bali']


class Config:
    QUEUE_SIZE = 10000
    QUEUE_TIMEOUT = 0.1
    DEFAULT_DURATION = 30
    DEFAULT_EVENTS_PER_SEC = 20
    WINDOW_SECONDS = 3
    ANOMALY_THRESHOLD = 300000


class StreamingPipeline:
    def __init__(self):
        self.event_queue: Queue = Queue(maxsize=Config.QUEUE_SIZE)
        self.state: Dict[str, Any] = {
            'total_events': 0,
            'total_revenue': 0.0,
            'events_by_type': defaultdict(int),
            'events_by_city': defaultdict(int),
            'revenue_by_city': defaultdict(float),
            'payments_by_city': defaultdict(int),
            'events_per_second': [],
            'timestamps': [],
            'cumulative_revenue': [],
            'revenue_alerts': []
        }
        self.shutdown_event = Event()
        self.producer_done = Event()

    @staticmethod
    def format_rupiah(value: float) -> str:
        if value == 0:
            return "Rp0"
        return f"Rp{value:,.0f}".replace(",", ".")

    def produce_events(self, duration_seconds: int, events_per_sec: int) -> None:
        logger.info(f"Producer started: {events_per_sec} events/sec for {duration_seconds}s")
        try:
            start = time.monotonic()
            event_id = 1
            while time.monotonic() - start < duration_seconds and not self.shutdown_event.is_set():
                event = {
                    'event_id': event_id,
                    'timestamp': datetime.now(),
                    'event_type': random.choice(EVENT_TYPES),
                    'city': random.choice(CITIES),
                    'order_value': round(random.uniform(10000, 500000), 2),
                    'user_id': random.randint(1000, 9999)
                }
                try:
                    self.event_queue.put(event, timeout=0.5)
                    event_id += 1
                except Exception as e:
                    logger.warning(f"Queue full, dropping event: {e}")
                self.shutdown_event.wait(min(1.0 / events_per_sec, max(0, duration_seconds - (time.monotonic() - start))))
        finally:
            self.producer_done.set()
            logger.info("Producer stopped")

    def flush_window(self, window_count: int, window_revenue: Dict[str, float], elapsed: float, final: bool = False) -> None:
        if not window_count:
            return
        eps = window_count / max(elapsed, 0.001)
        alerts = [
            {'city': city, 'revenue': revenue}
            for city, revenue in window_revenue.items()
            if revenue > Config.ANOMALY_THRESHOLD
        ]
        self.state['events_per_second'].append(eps)
        self.state['timestamps'].append(datetime.now())
        self.state['cumulative_revenue'].append(self.state['total_revenue'])
        self.state['revenue_alerts'].extend(alerts)
        label = 'Final window' if final else 'Window'
        logger.info(f"{label}: {window_count} events ({eps:.1f} events/sec) | Confirmed revenue: {self.format_rupiah(sum(window_revenue.values()))}")
        for alert in alerts:
            logger.warning(f"Revenue threshold alert: {alert['city']} reached {self.format_rupiah(alert['revenue'])} in this window")

    def consume_and_aggregate(self) -> None:
        logger.info(f"Consumer started: aggregating every {Config.WINDOW_SECONDS}s window")
        window_start = time.monotonic()
        window_count = 0
        window_revenue = defaultdict(float)

        while True:
            if self.producer_done.is_set() and self.event_queue.empty():
                break

            try:
                event = self.event_queue.get(timeout=Config.QUEUE_TIMEOUT)

                self.state['total_events'] += 1
                self.state['events_by_type'][event['event_type']] += 1
                self.state['events_by_city'][event['city']] += 1
                if event['event_type'] == 'payment_confirmed':
                    self.state['total_revenue'] += event['order_value']
                    self.state['revenue_by_city'][event['city']] += event['order_value']
                    self.state['payments_by_city'][event['city']] += 1
                    window_revenue[event['city']] += event['order_value']
                window_count += 1

                elapsed = time.monotonic() - window_start
                if elapsed >= Config.WINDOW_SECONDS:
                    self.flush_window(window_count, window_revenue, elapsed)
                    window_count = 0
                    window_revenue.clear()
                    window_start = time.monotonic()

            except Empty:
                elapsed = time.monotonic() - window_start
                if elapsed >= Config.WINDOW_SECONDS:
                    self.flush_window(window_count, window_revenue, elapsed)
                    window_count = 0
                    window_revenue.clear()
                    window_start = time.monotonic()
            except Exception as e:
                logger.error(f"Error processing event: {e}")

        self.flush_window(window_count, window_revenue, time.monotonic() - window_start, final=True)

        logger.info("Consumer stopped")

    def run(self, duration: float = Config.DEFAULT_DURATION, events_per_sec: float = Config.DEFAULT_EVENTS_PER_SEC) -> Dict[str, Any]:
        if not all(isfinite(value) and value > 0 for value in (duration, events_per_sec, Config.WINDOW_SECONDS)):
            raise ValueError("duration, events_per_sec, and WINDOW_SECONDS must be finite and positive")
        logger.info("Starting streaming pipeline...")
        producer_thread = Thread(target=self.produce_events, args=(duration, events_per_sec), daemon=False)
        consumer_thread = Thread(target=self.consume_and_aggregate, daemon=False)
        producer_thread.start()
        consumer_thread.start()
        try:
            while producer_thread.is_alive():
                producer_thread.join(timeout=0.1)
        except KeyboardInterrupt:
            logger.info("Shutdown requested")
            self.shutdown_event.set()
        finally:
            producer_thread.join()
            self.producer_done.set()
            consumer_thread.join()
        logger.info("Pipeline completed")
        return self.state

    def print_summary(self, duration: int) -> None:
        logger.info("=" * 70)
        logger.info("SUMMARY METRICS")
        logger.info("=" * 70)
        logger.info(f"Total Events Processed : {self.state['total_events']:,}")
        logger.info(f"Total Revenue          : {self.format_rupiah(self.state['total_revenue'])}")
        logger.info(f"Avg Events/sec         : {self.state['total_events']/duration:.1f}")

        payment_count = self.state['events_by_type']['payment_confirmed']
        if payment_count > 0:
            avg_order = self.state['total_revenue'] / payment_count
            logger.info(f"Avg Payment Value      : {self.format_rupiah(avg_order)}")

        if self.state['revenue_by_city']:
            top_city = max(self.state['revenue_by_city'], key=self.state['revenue_by_city'].get)
            logger.info(f"Top City by Revenue    : {top_city} ({self.format_rupiah(self.state['revenue_by_city'][top_city])})")

        if self.state['events_by_type']:
            top_event = max(self.state['events_by_type'], key=self.state['events_by_type'].get)
            logger.info(f"Most Frequent Event    : {top_event} ({self.state['events_by_type'][top_event]} events)")

    def print_revenue_alerts(self) -> List[Dict[str, Any]]:
        logger.info("=" * 70)
        logger.info(f"PER-WINDOW REVENUE ALERTS (Threshold: {self.format_rupiah(Config.ANOMALY_THRESHOLD)})")
        logger.info("=" * 70)
        alerts = self.state['revenue_alerts']
        if alerts:
            logger.info(f"Triggered {len(alerts)} alert(s):")
            for alert in alerts:
                logger.info(f"  {alert['city']:<15} Window revenue: {self.format_rupiah(alert['revenue'])}")
        else:
            logger.info("No revenue threshold alerts triggered")
        return alerts

    def visualize_results(self) -> None:
        logger.info("=" * 70)
        logger.info("ANALYTICS DASHBOARD")
        logger.info("=" * 70)

        if self.state['events_by_type']:
            logger.info("Event Type Distribution:")
            logger.info("-" * 70)
            for event_type, count in sorted(self.state['events_by_type'].items(), key=lambda x: x[1], reverse=True):
                pct = (count / self.state['total_events']) * 100 if self.state['total_events'] > 0 else 0
                bar = "█" * int(pct / 2)
                logger.info(f"{event_type:25} {count:5} ({pct:5.1f}%) {bar}")

        if self.state['revenue_by_city']:
            logger.info("\nRevenue by City (Detail):")
            logger.info("-" * 80)
            logger.info(f"{'City':<15} {'Total Revenue':<25} {'Payments':<10} {'Avg Payment':<20}")
            logger.info("-" * 80)
            for city in sorted(self.state['revenue_by_city'].keys(), 
                              key=lambda x: self.state['revenue_by_city'][x], 
                              reverse=True):
                total_rev = self.state['revenue_by_city'][city]
                events_count = self.state['payments_by_city'][city]
                avg_order = total_rev / events_count if events_count > 0 else 0

                logger.info(f"{city:<15} {self.format_rupiah(total_rev):<25} {events_count:<10} {self.format_rupiah(avg_order):<20}")


def main() -> None:
    pipeline = StreamingPipeline()

    duration = Config.DEFAULT_DURATION
    events_per_sec = Config.DEFAULT_EVENTS_PER_SEC

    pipeline.run(duration=duration, events_per_sec=events_per_sec)

    pipeline.print_summary(duration)
    pipeline.print_revenue_alerts()
    pipeline.visualize_results()

    logger.info("All done!")


if __name__ == "__main__":
    main()
