import json 
import time

from confluent_kafka import Producer
from utils.event_generator import generate_malform_events
import random

EVENT_DISTRIBUTION = {
        "valid": 50,
        "invalid_json": 10,
        "missing_fields": 5,
        "wrong_data_types": 5,
        "invalid_values": 5,
        "invalid_timestamp": 5,
    }

stats = {
    "delivered":0,
    "failed":0
}

def order_report(err,msg):
    if err is None:
        stats["delivered"] += 1
    else:
        stats["failed"] += 1


def malformed_events_production():
    events = 10000
    rate = 1000 
    producer = Producer({
        "bootstrap.servers": "localhost:9094",
        "client.id" : "marktstorm-malformed-events-production-generator",
        "acks": "all"
    })
    event_types = []
    for event_type, prob in EVENT_DISTRIBUTION.items():
        count = events * prob // 100
        event_types.extend([event_type]*count)
    random.shuffle(event_types)
    
    start_time = time.perf_counter()

    for i , event_type in enumerate(event_types,start=1):
        target_time = start_time + ((i-1)/rate)
        while time.perf_counter() < target_time:
            time.sleep(
                max(0,min(target_time-time.perf_counter(),0.001))
            )
        event,payload = generate_malform_events(i,event_type=event_type)
        send_timestamp = time.time_ns() // 1_000_000

        key = event["user_id"].encode("utf-8")

        while True:
            try:
                producer.produce(
                    "orders",
                    key=key,
                    value = payload,
                    callback=order_report
                )
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)

    remaining_orders = producer.flush(30)
    total_time = time.perf_counter() - start_time


if __name__ == "__main__":
    malformed_events_production()