#!/bin/bash

declare -a args=(0 1 2 3)
declare -A pids
command_file="/tmp/revive_command"

show_status() {
    echo -e "\n=== Script Status ==="
    for arg in "${args[@]}"; do
        pid=$(pgrep -f "python3 node.py $arg")
        if [ -n "$pid" ]; then
            echo "Script with argument $arg is running (PID: $pid)"
            pids[$arg]=$pid
        else
            echo "Script with argument $arg is not running."
            unset pids[$arg]
        fi
    done
    echo "======================"
}

kill_script() {
    local arg=$1
    if [ -n "${pids[$arg]}" ] && kill -0 ${pids[$arg]} 2>/dev/null; then
        echo "Killing script with argument: $arg"
        kill ${pids[$arg]}
        unset pids[$arg]
        echo "Script with argument $arg has been stopped."
    else
        echo "Script with argument $arg is not running."
    fi
}

revive_script() {
    local arg=$1
    echo "Sending revive command for script with argument: $arg"
    echo "$arg" >>"$command_file"
}

manage_scripts() {
    while true; do
        show_status
        echo -e "\nOptions:"
        echo "1. Kill a script"
        echo "2. Revive a script"
        echo "3. Exit"
        read -p "Enter your choice: " choice
        case $choice in
            1)
                read -p "Enter the argument of the script to kill (0-3): " arg
                if [[ " ${args[*]} " =~ " $arg " ]]; then
                    kill_script $arg
                else
                    echo "Invalid argument."
                fi
                ;;
            2)
                read -p "Enter the argument of the script to revive (0-3): " arg
                if [[ " ${args[*]} " =~ " $arg " ]]; then
                    revive_script $arg
                else
                    echo "Invalid argument."
                fi
                ;;
            3)
                echo "Exiting script monitor."
                exit 0
                ;;
            *)
                echo "Invalid choice. Please enter 1, 2, or 3."
                ;;
        esac
    done
}

> "$command_file"

manage_scripts
