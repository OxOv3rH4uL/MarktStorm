import asyncio
from .order_consumer import get_order_consumer
from .product_consumer import get_product_consumer
from .user_consumer import get_user_consumer
from .dead_letter_consumer import get_dead_letter_consumer
from concurrent.futures import ThreadPoolExecutor
from database.db_writer import DuckDBWriter


async def writerz(message_queue, writer, executor):
    loop = asyncio.get_running_loop()

    while True:
        item = await message_queue.get()
        try:
            if item["type"] == "order":
                method = writer.insert_order_metrics

            elif item["type"] == "product":
                method = writer.insert_product_metrics

            elif item["type"] == "user":
                method = writer.insert_user_metrics

            elif item["type"] == "dead-letter":
                method = writer.insert_dead_letter

            else:
                raise ValueError("Unknown type present in the message queue")

            await loop.run_in_executor(
                executor,
                method,
                item["data"],
                item["topic"],
                item["partition"],
                item["offset"]
            )
            item["ack"].set_result(True)
        except Exception as e:
            item["ack"].set_exception(e)

        finally:
            message_queue.task_done()



async def snapshot_scheduler(writer,executor):
    loop = asyncio.get_running_loop()

    while True:
        await asyncio.sleep(10)

        await loop.run_in_executor(
            executor,
            writer.create_snapshot
        )




async def main():
    message_queue = asyncio.Queue(maxsize=10000)
    executor = ThreadPoolExecutor(max_workers=1)
    loop = asyncio.get_running_loop()
    writer = await loop.run_in_executor(
        executor,
        DuckDBWriter
    )
    asyncio.create_task(
        writerz(message_queue,writer,executor)
    )
    
    consumers_scheduler = [
        get_order_consumer(message_queue),
        get_product_consumer(message_queue),
        get_user_consumer(message_queue),
        get_dead_letter_consumer(message_queue),
        snapshot_scheduler(writer,executor)
    ]
    try:
        await asyncio.gather(
            *consumers_scheduler
        )
    except Exception as e:
        print(e)

if __name__ == "__main__":
    asyncio.run(main())
