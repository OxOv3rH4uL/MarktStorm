import json
from confluent_kafka import Consumer, KafkaException

consumer_config = {
    "bootstrap.servers" : "localhost:9094",
    "group.id" : "marktstorm-user-consumers",
    "auto.offset.reset" : "earliest",
    "enable.auto.commit" : False 
}

consumer = Consumer(consumer_config)

consumer.subscribe(["user-aggregation"])

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        try:
            msgz = msg.value().decode("utf-8")
            order = json.loads(msgz)
            consumer.commit(message = msg, asynchronous=False)

        except:
            print("Invalid Messages Incoming. Should Check Dead Letter Producer")
except KeyboardInterrupt:
    print("Stopping consumer")

finally:
    consumer.close()


        