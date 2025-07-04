import asyncio
import aio_pika
import redis.asyncio as redis

async def test_connections():
    # test rabbitmq connection
    try:
        connection = await aio_pika.connect_robust("amqp://admin:admin123@localhost:5672/")
        print("rabbitmq connection success")
        await connection.close()
    except Exception as e:
        print(f"rabbitmq connection failed: {e}")

    # test redis connection
    try:
        redis_client = redis.from_url("redis://localhost:6379/0")
        await redis_client.ping()
        print("redis connection success")
        await redis_client.close()
    except Exception as e:
        print(f"redis connection failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_connections())