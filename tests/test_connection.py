#!/usr/bin/env python3
"""
Connection test script
Tests RabbitMQ and Redis connections

REQUIRED SERVICES:
Before running this test, ensure the following services are running:

1. RabbitMQ (port 5672):
   docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
   # Or use docker-compose: docker-compose up -d rabbitmq

2. Redis (port 6379):
   docker run -d --name redis -p 6379:6379 redis:7-alpine
   # Or use docker-compose: docker-compose up -d redis

3. Or start both services with docker-compose:
   docker-compose up -d rabbitmq redis
"""

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