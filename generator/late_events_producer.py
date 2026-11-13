import json 
import time

from confluent_kafka import Producer
from utils.event_generator import generate_late_events

stats = {
    "delivered":0,
    "failed":0
}

def order_report(err,msg):
    if err is None:
        stats["delivered"] += 1
    else:
        stats["failed"] += 1


def late_event_production():
    producer = Producer({
        "bootstrap.servers": "localhost:9094",
        "client.id" : "marktstorm-late-event-production-generator",
        "acks": "all"
    })
    events = 10000
    rate = 1000 
    event_number = 0
    start_time = time.perf_counter()
    for batch in range(events//rate):
        eventz = []
        for j in range(rate):
            event_number+=1
            event = generate_late_events(event_number)
            eventz.append(event)

        eventz.sort(
            key=lambda e: e["event_timestamp"],
            reverse=True
        )

        for event_i in eventz:
            # target_time = start_time + (i/rate)
            
            key = event_i["user_id"]
            payload = json.dumps(event_i).encode("utf-8")
            while True:
                try:
                    send_timestamp = time.time_ns() // 1_000_000
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

    print("\n###### Late Event Production Results ######")
    print(f"Scheduled events: {events}")
    print(f"Target rate: {rate} events/sec")
    print(f"Delivered: {stats['delivered']}")
    print(f"Failed: {stats['failed']}")
    print(f"Outstanding messages: {remaining_orders}")
    print(f"Total duration: {total_time:.2f} seconds")
    print(f"Actual delivery throughput: {stats['delivered'] / total_time:.2f} events/sec")


if __name__ == "__main__":
    late_event_production()