#!/bin/bash

# Stop all distributed workers
echo "Stopping all workers..."

# Find and kill all worker processes
pids=$(ps aux | grep "distributed_worker.py" | grep -v grep | awk '{print $2}')

if [ -z "$pids" ]; then
    echo "No workers found running"
else
    echo "Found worker PIDs: $pids"
    echo "$pids" | xargs kill -TERM
    echo "Sent TERM signal to all workers"
    
    # Wait and force kill if needed
    sleep 3
    remaining_pids=$(ps aux | grep "distributed_worker.py" | grep -v grep | awk '{print $2}')
    if [ ! -z "$remaining_pids" ]; then
        echo "Force killing remaining workers: $remaining_pids"
        echo "$remaining_pids" | xargs kill -KILL
    fi
fi

echo "All workers stopped!" 