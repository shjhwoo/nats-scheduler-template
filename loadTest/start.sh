#!/bin/bash

TOTAL_MESSAGES=$1
OVERWRITE_COUNT=$2
INTERVAL_MINUTES=$3
MESSAGES_PER_INTERVAL=$4

if ! command -v jq &> /dev/null
then
    echo "Error: jq is required for JSON parsing. Please install it (e.g., sudo dnf install jq)."
    exit 1
fi

monitor_curl_jsz() {
    local LOG_FILE="loadResult.log"
    echo "=== NATS JSZ Monitoring Started: $(date) ===" > "$LOG_FILE"

    while true; do
        echo "--- $(date) ---" >> "$LOG_FILE"

        # get memory, cpu usage and save to log file
        curl -s http://localhost:8222/varz?js=true | jq '{mem: .mem, cpu: .cpu}' >> "$LOG_FILE"

        sleep 30
    done
}

monitor_curl_jsz &
CURL_MONITOR_PID=$!
echo "CURL JSZ Monitor started with PID: $CURL_MONITOR_PID (Logging to loadResult.log)"

echo "----------------------------------------"
echo "Load Test is running..."

go run main.go $TOTAL_MESSAGES $OVERWRITE_COUNT $INTERVAL_MINUTES $MESSAGES_PER_INTERVAL
LOAD_TEST_EXIT_CODE=$?
echo "? finished loadTestCode (exit code: $LOAD_TEST_EXIT_CODE)"
echo "----------------------------------------"

echo "quit Monitoring process..."
kill $CURL_MONITOR_PID 2>/dev/null

echo "========================================="

exit $LOAD_TEST_EXIT_CODE