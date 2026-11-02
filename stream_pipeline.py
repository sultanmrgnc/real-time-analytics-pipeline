import time
import random
import logging
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
            'events_per_second': [],
            'timestamps': [],
            'cumulative_revenue': []
        }
        self.shutdown_event = Event()

    @staticmethod
    def format_rupiah(value: float) -> str:
        if value == 0:
            return "Rp0"
        return f"Rp{value:,.0f}".replace(",", ".")

    def produce_events(self, duration_seconds: int, events_per_sec: int) -> None:
        logger.info(f"Producer started: {events_per_sec} events/sec for {duration_seconds}s")
        start = time.time()
        event_id = 1

        while time.time() - start < duration_seconds and not self.shutdown_event.is_set():
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

            time.sleep(1.0 / events_per_sec)

        logger.info("Producer stopped")

    def consume_and_aggregate(self, duration_seconds: int) -> None:
        logger.info(f"Consumer started: aggregating every {Config.WINDOW_SECONDS}s window")
        start = time.time()
        window_start = time.time()
        window_count = 0

        while time.time() - start < duration_seconds and not self.shutdown_event.is_set():
            try:
                event = self.event_queue.get(timeout=Config.QUEUE_TIMEOUT)

                self.state['total_events'] += 1
                self.state['total_revenue'] += event['order_value']
                self.state['events_by_type'][event['event_type']] += 1
                self.state['events_by_city'][event['city']] += 1
                self.state['revenue_by_city'][event['city']] += event['order_value']
                window_count += 1

                if time.time() - window_start >= Config.WINDOW_SECONDS:
                    eps = window_count / Config.WINDOW_SECONDS
                    self.state['events_per_second'].append(eps)
                    self.state['timestamps'].append(datetime.now())
                    self.state['cumulative_revenue'].append(self.state['total_revenue'])

                    logger.info(
                        f"Window: {window_count} events ({eps:.1f} events/sec) | "
                        f"Total revenue: {self.format_rupiah(self.state['total_revenue'])}"
                    )
                    window_count = 0
                    window_start = time.time()

            except Empty:
                pass
            except Exception as e:
                logger.error(f"Error processing event: {e}")

        logger.info("Consumer stopped")

    def run(self, duration: int = Config.DEFAULT_DURATION, events_per_sec: int = Config.DEFAULT_EVENTS_PER_SEC) -> Dict[str, Any]:
        logger.info("Starting streaming pipeline...")

        producer_thread = Thread(target=self.produce_events, args=(duration, events_per_sec), daemon=False)
        consumer_thread = Thread(target=self.consume_and_aggregate, args=(duration,), daemon=False)

        try:
            producer_thread.start()
            consumer_thread.start()

            producer_thread.join(timeout=duration + 5)
            consumer_thread.join(timeout=duration + 5)
        finally:
            self.shutdown_event.set()

        logger.info("Pipeline completed")
        return self.state

    def print_summary(self, duration: int) -> None:
        logger.info("=" * 70)
        logger.info("SUMMARY METRICS")
        logger.info("=" * 70)
        logger.info(f"Total Events Processed : {self.state['total_events']:,}")
        logger.info(f"Total Revenue          : {self.format_rupiah(self.state['total_revenue'])}")
        logger.info(f"Avg Events/sec         : {self.state['total_events']/duration:.1f}")

        if self.state['total_events'] > 0:
            avg_order = self.state['total_revenue'] / self.state['total_events']
            logger.info(f"Avg Order Value        : {self.format_rupiah(avg_order)}")

        if self.state['revenue_by_city']:
            top_city = max(self.state['revenue_by_city'], key=self.state['revenue_by_city'].get)
            logger.info(f"Top City by Revenue    : {top_city} ({self.format_rupiah(self.state['revenue_by_city'][top_city])})")

        if self.state['events_by_type']:
            top_event = max(self.state['events_by_type'], key=self.state['events_by_type'].get)
            logger.info(f"Most Frequent Event    : {top_event} ({self.state['events_by_type'][top_event]} events)")

    def detect_anomalies(self, threshold: int = Config.ANOMALY_THRESHOLD) -> List[Dict[str, Any]]:
        logger.info("=" * 70)
        logger.info(f"ANOMALY DETECTION (Threshold: {self.format_rupiah(threshold)})")
        logger.info("=" * 70)

        anomalies = []
        for city, revenue in self.state['revenue_by_city'].items():
            if revenue > threshold:
                events_count = self.state['events_by_city'][city]
                anomalies.append({
                    'city': city,
                    'total_revenue': revenue,
                    'avg_order': revenue / events_count if events_count > 0 else 0
                })

        if anomalies:
            anomalies_sorted = sorted(anomalies, key=lambda x: x['total_revenue'], reverse=True)
            logger.info(f"Found {len(anomalies)} city/cities with high revenue:")
            for a in anomalies_sorted:
                logger.info(
                    f"  {a['city']:<15} Total: {self.format_rupiah(a['total_revenue']):<25} "
                    f"Avg: {self.format_rupiah(a['avg_order'])}"
                )
        else:
            logger.info("No anomalies detected")

        return anomalies

    def visualize_results(self) -> None:
        try:
            import plotext as plt
        except ImportError:
            logger.warning("plotext not installed. Install with: pip install plotext")
            logger.info("Showing text-based visualization only")
            plt = None

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
            logger.info(f"{'City':<15} {'Total Revenue':<25} {'Events':<10} {'Avg Order':<20}")
            logger.info("-" * 80)
            for city in sorted(self.state['revenue_by_city'].keys(), 
                              key=lambda x: self.state['revenue_by_city'][x], 
                              reverse=True):
                total_rev = self.state['revenue_by_city'][city]
                events_count = self.state['events_by_city'][city]
                avg_order = total_rev / events_count if events_count > 0 else 0

                logger.info(f"{city:<15} {self.format_rupiah(total_rev):<25} {events_count:<10} {self.format_rupiah(avg_order):<20}")


def main() -> None:
    pipeline = StreamingPipeline()

    duration = Config.DEFAULT_DURATION
    events_per_sec = Config.DEFAULT_EVENTS_PER_SEC

    state = pipeline.run(duration=duration, events_per_sec=events_per_sec)

    pipeline.print_summary(duration)
    pipeline.detect_anomalies(threshold=Config.ANOMALY_THRESHOLD)
    pipeline.visualize_results()

    logger.info("All done!")


if __name__ == "__main__":
    main()
