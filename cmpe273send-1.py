import pika
import asyncio

def send_message(message):
    """Sends a single message to RabbitMQ synchronously."""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        # Consistent queue name (task_queue)
        channel.queue_declare(queue='task_queue', durable=True)
        channel.basic_publish(
            exchange='',
            routing_key='task_queue',
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)  # Persistent
        )
        print(f"Message sent: {message}")
        connection.close()
    except Exception as e:
        print(f"Error sending message: {e}")

async def send_messages():
    """Asynchronously sends 1,000,000 messages."""
    for i in range(1000000):
        message = f"Message {i}"
        await asyncio.to_thread(send_message, message)  # Run synchronously within asyncio
        if i % 10000 == 0:
            print(f"Sent {i} messages...")
    print("All messages sent.")

if __name__ == "__main__":
    asyncio.run(send_messages())
