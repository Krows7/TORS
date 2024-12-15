#!/bin/bash

declare -a pids
declare -a args=(0 1 2 3)
command_file="/tmp/revive_command"

start_script() {
    local arg=$1
    echo "Starting script with argument: $arg"
    python3 node.py $arg &
    pids[$arg]=$!
}

check_for_commands() {
    if [ -f "$command_file" ]; then
        while read -r cmd; do
            echo "Received command: $cmd"
            start_script "$cmd"
        done <"$command_file"
        >"$command_file"
    fi
}

cleanup() {
    echo "Stopping all scripts..."
    for pid in "${pids[@]}"; do
        if kill -0 $pid 2>/dev/null; then
            kill $pid
        fi
    done
    rm -f "$command_file"
    echo "All scripts stopped."
    exit 0
}

trap cleanup SIGINT

for arg in "${args[@]}"; do
    start_script $arg
done

echo "Monitoring for commands. Press CTRL+C to stop."
while true; do
    check_for_commands
    sleep 0.5
done
