import json
from confluent_kafka import Consumer, KafkaException
import asyncio

async def get_user_consumer(message_queue):
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
            msg = consumer.poll(0)

            if msg is None:
                await asyncio.sleep(0.01)
                continue
            if msg.error():
                raise KafkaException(msg.error())

            try:
                msgz = msg.value().decode("utf-8")
                order = json.loads(msgz)
                ack = asyncio.get_running_loop().create_future()

                await message_queue.put({
                    "type":"user",
                    "data" : order,
                    "topic" : msg.topic(),
                    "partition" : msg.partition(),
                    "offset" : msg.offset(),
                    "ack" : ack
                })
                await ack

                consumer.commit(message = msg, asynchronous=False)

            except Exception as e:
                print(e)
    except asyncio.CancelledError:
        print("Stopping consumer")

    finally:
        consumer.close()



