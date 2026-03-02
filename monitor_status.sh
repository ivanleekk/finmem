#!/bin/bash

echo "================================================================="
echo "                 FINMEM SIMULATION STATUS DASHBOARD              "
echo "================================================================="

# 1. Determine which Job ID to monitor
if [ -n "$1" ]; then
    # Use the Job ID provided by the user
    TARGET_JOBID="$1"
else
    # No ID provided: Isolate the most recent SLURM Job ID
    TARGET_JOBID=$(ls runs/sim_*.out 2>/dev/null | grep -o 'sim_[0-9]*_' | grep -o '[0-9]*' | sort -nu | tail -n 1)
fi

if [ -z "$TARGET_JOBID" ]; then
    echo "No output logs found in the runs/ directory."
    echo "================================================================="
    exit 0
fi

echo "Monitoring Job Array ID: $TARGET_JOBID"
echo "--- Current Progress ---"

# 2. Check Standard Output for the target Job ID
shopt -s nullglob
OUT_FILES=(runs/sim_${TARGET_JOBID}_*.out)

if [ ${#OUT_FILES[@]} -eq 0 ]; then
    echo "No logs found for Job ID: $TARGET_JOBID. Check if the ID is correct."
    echo "================================================================="
    exit 1
fi

for file in "${OUT_FILES[@]}"; do
    # Extract the Array Task ID from the filename (e.g., the '1' in sim_12345_1.out)
    TASK_ID=$(basename "$file" | sed -n "s/sim_${TARGET_JOBID}_\(.*\)\.out/\1/p")
    
    # Grab the very last progress marker printed in the log
    LATEST_STATUS=$(grep ">>>" "$file" | tail -n 1)
    
    # If no marker is found yet, it's booting up
    if [ -z "$LATEST_STATUS" ]; then
        LATEST_STATUS="Initializing environment / Loading data..."
    fi
    
    echo "Task [$TASK_ID]: $LATEST_STATUS"
done

echo ""
echo "--- Error Log Check ---"

# 3. Check Standard Error for the target Job ID
ERR_FILES=(runs/sim_${TARGET_JOBID}_*.err)
ERRORS_FOUND=0

for err_file in "${ERR_FILES[@]}"; do
    # Check if the error file exists and has a size greater than 0
    if [ -s "$err_file" ]; then
        TASK_ID=$(basename "$err_file" | sed -n "s/sim_${TARGET_JOBID}_\(.*\)\.err/\1/p")
        FILE_SIZE=$(du -h "$err_file" | cut -f1)
        
        echo "⚠️  Task [$TASK_ID] has generated errors/warnings ($FILE_SIZE). Last 3 lines:"
        tail -n 3 "$err_file" | sed 's/^/    /'
        echo ""
        ERRORS_FOUND=1
    fi
done

if [ $ERRORS_FOUND -eq 0 ]; then
    echo "✅ No errors detected in the targeted run's .err logs!"
fi
echo "================================================================="
