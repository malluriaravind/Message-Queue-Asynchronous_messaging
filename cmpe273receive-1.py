import aio_pika
import asyncio

async def process_message(message):
    """Asynchronous message handler."""
    try:
        async with message.process():
            print(f"Received: {message.body.decode()}")
    except Exception as e:
        print(f"Error processing message: {e}")

async def consume_messages():
    """Consumes messages asynchronously from RabbitMQ."""
    try:
        connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
        channel = await connection.channel()

        queue = await channel.declare_queue("task_queue", durable=True)
        print("Waiting for messages. To exit press CTRL+C")

        await queue.consume(process_message)

        # Keep the consumer running
        await asyncio.Future()
    except Exception as e:
        print(f"Error in consumer setup: {e}")

if __name__ == "__main__":
    asyncio.run(consume_messages())
