import json 
import time

from confluent_kafka import Producer
from utils.event_generator import generate

stats = {
    "delivered":0,
    "failed":0
}

def order_report(err,msg):
    if err is None:
        stats["delivered"] += 1
    else:
        stats["failed"] += 1


def normal_production():
    producer = Producer({
        "bootstrap.servers": "localhost:9094",
        "client.id" : "marktstorm-normal-production-generator",
        "acks": "all"
    })
    start_time = time.perf_counter()
    events = 10000
    rate = 1000 
    for i in range(events):
        target_time = start_time + (i/rate)
        while time.perf_counter() < target_time:
            time.sleep(
                max(0,min(target_time-time.perf_counter(),0.001))
            )
        event = generate(i+1)
        key = event["user_id"]
        payload = json.dumps(event).encode("utf-8")
        while True:
            try:
                producer.produce(
                    "orders",
                    key=key.encode("utf-8"),
                    value = payload,
                    callback=order_report
                )
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)

    remaining_orders = producer.flush(30)
    total_time = time.perf_counter() - start_time

    print("\n###### Normal Production Results ######")
    print(f"Scheduled events: {events}")
    print(f"Target rate: {rate} events/sec")
    print(f"Delivered: {stats['delivered']}")
    print(f"Failed: {stats['failed']}")
    print(f"Outstanding messages: {remaining_orders}")
    print(f"Total duration: {total_time:.2f} seconds")
    print(f"Actual delivery throughput: {stats['delivered'] / total_time:.2f} events/sec")


if __name__ == "__main__":
    normal_production()