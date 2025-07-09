#!/bin/bash

# Start multiple distributed workers
# Usage: ./start_workers.sh [num_workers]

NUM_WORKERS=${1:-3}  # Default to 3 workers if not specified

# Store PIDs for cleanup
PIDS=()

# Function to cleanup workers
cleanup() {
    echo ""
    echo "Received interrupt signal, stopping all workers..."
    
    # Stop all workers
    for pid in "${PIDS[@]}"; do
        if kill -0 $pid 2>/dev/null; then
            echo "Stopping worker with PID: $pid"
            kill -TERM $pid
        fi
    done
    
    # Wait for graceful shutdown
    sleep 3
    
    # Force kill remaining processes
    remaining_pids=$(ps aux | grep "distributed_worker.py" | grep -v grep | awk '{print $2}')
    if [ ! -z "$remaining_pids" ]; then
        echo "Force killing remaining workers: $remaining_pids"
        echo "$remaining_pids" | xargs kill -KILL
    fi
    
    echo "All workers stopped!"
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

echo "Starting $NUM_WORKERS workers..."

# Add project root to PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Start workers in background
for i in $(seq 1 $NUM_WORKERS); do
    echo "Starting worker-$i..."
    python worker/distributed_worker.py --worker-id "worker-$i" --log-level INFO &
    pid=$!
    PIDS+=($pid)
    echo "Worker-$i started with PID: $pid"
done

echo "All $NUM_WORKERS workers started!"
echo "Press Ctrl+C to stop all workers"
echo "To stop all workers manually, run: ./stop_workers.sh"

# Wait for interrupt signal
wait 