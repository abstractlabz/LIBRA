#!/bin/bash

# Start all services in the background
echo "Starting services..."

# Start Go services
#echo "Starting connect-coinbase on port 6070..."
#cd /app/user/coinbase
# Copy SSL certificates to the current directory
#cp /app/server.crt . && cp /app/server.key .
#go run connect-coinbase.go &
#COINBASE_PID=$!

echo "Starting connect-schwab on port 5003..."
cd /app/user/schwab
# Copy SSL certificates to the current directory
cp /app/server.crt . && cp /app/server.key .
python3 connect-schwab.py &
SCHWAB_PID=$!

# Start other Go services
cd /app/algo && go run rebalancer.go &
cd /app/pipeline/api/llm && go run inferencer.go &
cd /app/pipeline/api/normalize && go run normalization.go &

echo "Starting get-portfolio on port 8080..."
cd /app/user/utils && go run get-portfolio.go &
PORTFOLIO_PID=$!

# Start Python services
#cd /app/pipeline/api/coinbase && python3 coinbase.py &
cd /app/pipeline/api/schwab && python3 schwab.py &

# Function to check if a port is listening
check_port() {
    local port=$1
    local service=$2
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if netstat -tuln | grep ":$port " > /dev/null; then
            echo "$service is listening on port $port"
            return 0
        fi
        echo "Waiting for $service to start on port $port (attempt $attempt/$max_attempts)..."
        sleep 2
        attempt=$((attempt + 1))
    done
    echo "Error: $service failed to start on port $port"
    return 1
}

# Check if critical services are running
#check_port 6070 "connect-coinbase" || exit 1
check_port 5003 "connect-schwab" || exit 1
check_port 8080 "get-portfolio" || exit 1

echo "All services started successfully!"

# Wait for all background processes
wait
