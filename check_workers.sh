#!/bin/bash

# Check distributed worker status
echo "Checking worker status..."

# Check running processes
echo "Process Status:"
worker_processes=$(ps aux | grep "distributed_worker.py" | grep -v grep)

if [ -z "$worker_processes" ]; then
    echo "No worker processes found running"
else
    echo "Found running workers:"
    echo "$worker_processes"
fi

echo ""

# Check Redis for registered workers
echo "Redis Worker Registry:"
redis_workers=$(redis-cli KEYS "worker:*" 2>/dev/null)

if [ -z "$redis_workers" ]; then
    echo "No workers registered in Redis"
else
    echo "Found registered workers in Redis:"
    for key in $redis_workers; do
        worker_id=$(echo $key | sed 's/worker://')
        worker_info=$(redis-cli GET "$key" 2>/dev/null)
        if [ ! -z "$worker_info" ]; then
            echo "Worker: $worker_id"
            echo "$worker_info" | python -m json.tool 2>/dev/null || echo "$worker_info"
            echo ""
        fi
    done
fi

echo "Status check completed!" 