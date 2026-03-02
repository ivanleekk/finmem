#!/bin/bash
#SBATCH --job-name=finmem_sim
#SBATCH --output=runs/sim_%A_%a.out
#SBATCH --error=runs/sim_%A_%a.err
#SBATCH --partition=long
#SBATCH --time=3-00:00:00        # UPDATED: Max time of 3 days (days-hh:mm:ss)
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4      
#SBATCH --mem=16G              
#SBATCH --array=1-16           # 16 tasks handling 2 files each
#SBATCH --mail-type=ALL
#SBATCH --mail-user=ivan.lee@u.nus.edu

# Grab all environment files into an array
FILES=(data/03_model_input/env_data_*.pkl)

# Calculate which 2 files this specific task should process
# e.g., Task 1 processes indices 0 and 1. Task 2 processes 2 and 3.
START_IDX=$(( (SLURM_ARRAY_TASK_ID - 1) * 2 ))
END_IDX=$(( START_IDX + 1 ))

echo "Task $SLURM_ARRAY_TASK_ID handling indices $START_IDX to $END_IDX"

for i in $(seq $START_IDX $END_IDX); do
    # Ensure we don't go out of bounds if there's an odd number of files
    if [ $i -lt ${#FILES[@]} ]; then
        FILE="${FILES[$i]}"
        
        # Extract ticker name
        BASENAME=$(basename "$FILE")
        TICKER="${BASENAME#env_data_}"
        TICKER="${TICKER%.pkl}"

        echo "========================================"
        echo "Processing Ticker: $TICKER"
        echo "Using Data File: $FILE"
        echo "========================================"

        # Define run-specific paths
        CONFIG_PATH="config/generated_configs/${TICKER}_config.toml"
        TRAIN_RESULT="data/05_train_model_output/$TICKER"
        TRAIN_CKPT="data/06_train_checkpoint/$TICKER"
        TEST_RESULT="data/07_test_model_output/$TICKER"
        TEST_CKPT="data/08_test_checkpoint/$TICKER"

        # Create directories
        mkdir -p "$TRAIN_RESULT" "$TRAIN_CKPT" "$TEST_RESULT" "$TEST_CKPT"

        # Run Training Phase (2019)
        echo ">>> Starting TRAINING phase for $TICKER..."
        uv run run.py sim \
            --market-data-path "$FILE" \
            --start-time "2019-01-01" \
            --end-time "2019-12-31" \
            --run-model "train" \
            --config-path "$CONFIG_PATH" \
            --checkpoint-path "$TRAIN_CKPT" \
            --result-path "$TRAIN_RESULT"

        # Run Testing Phase (2020-2024)
        echo ">>> Starting TESTING phase for $TICKER..."
        uv run run.py sim \
            --market-data-path "$FILE" \
            --start-time "2020-01-01" \
            --end-time "2024-12-31" \
            --run-model "test" \
            --config-path "$CONFIG_PATH" \
            --checkpoint-path "$TEST_CKPT" \
            --result-path "$TEST_RESULT" \
            --trained-agent-path "$TRAIN_CKPT"

        echo ">>> Finished processing $TICKER."
    fi
done
