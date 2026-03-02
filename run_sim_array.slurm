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

set -u

run_sim() {
    local mode="$1"
    local data_file="$2"
    local start_date="$3"
    local end_date="$4"
    local config_path="$5"
    local checkpoint_path="$6"
    local result_path="$7"
    local trained_path="${8:-}"

    if [ "$mode" = "test" ]; then
        echo "[CMD] uv run run.py sim --market-data-path $data_file --start-time $start_date --end-time $end_date --run-model $mode --config-path $config_path --checkpoint-path $checkpoint_path --result-path $result_path --trained-agent-path $trained_path"
        uv run run.py sim --market-data-path "$data_file" --start-time "$start_date" --end-time "$end_date" --run-model "$mode" --config-path "$config_path" --checkpoint-path "$checkpoint_path" --result-path "$result_path" --trained-agent-path "$trained_path"
    else
        echo "[CMD] uv run run.py sim --market-data-path $data_file --start-time $start_date --end-time $end_date --run-model $mode --config-path $config_path --checkpoint-path $checkpoint_path --result-path $result_path"
        uv run run.py sim --market-data-path "$data_file" --start-time "$start_date" --end-time "$end_date" --run-model "$mode" --config-path "$config_path" --checkpoint-path "$checkpoint_path" --result-path "$result_path"
    fi
}

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
        run_sim "train" "$FILE" "2019-01-01" "2019-12-31" "$CONFIG_PATH" "$TRAIN_CKPT" "$TRAIN_RESULT"

        # Run Testing Phase (2020-2024)
        echo ">>> Starting TESTING phase for $TICKER..."
        run_sim "test" "$FILE" "2020-01-01" "2024-12-31" "$CONFIG_PATH" "$TEST_CKPT" "$TEST_RESULT" "$TRAIN_CKPT"

        echo ">>> Exporting backtest decisions for $TICKER..."
        if [ -d "$TEST_RESULT/agent_1" ]; then
            uv run export_results.py --base-dir "$TEST_RESULT"
            echo ">>> Backtest file pattern: ${TICKER}_decisions_<start>_<end>_finmem.csv"
        else
            echo "[WARN] Missing test result checkpoint at $TEST_RESULT/agent_1, skip export for $TICKER"
        fi

        echo ">>> Finished processing $TICKER."
    fi
done
