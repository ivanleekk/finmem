#!/bin/bash

# OpenRouter API key (used for chat completions)
export OPENROUTER_API_KEY="Enter your OpenRouter API key here"

# OpenAI API key is still required for text-embedding-ada-002 embeddings
export OPENAI_API_KEY="Enter your OpenAI API key here"

TICKER="TSLA"
PKL_PATH="data/03_model_input/env_data_${TICKER}.pkl"
CONFIG="config/tsla_openrouter_config.toml"

# 1-week training window
TRAIN_START="2024-01-08"
TRAIN_END="2024-01-14"

TRAIN_CHECKPOINT="./data/06_train_checkpoint"
TRAIN_OUTPUT="./data/05_train_model_output"
LOG_DIR="./data/04_model_output_log"

mkdir -p "$TRAIN_CHECKPOINT" "$TRAIN_OUTPUT" "$LOG_DIR"

echo "=========================================="
echo "FinMem OpenRouter Test Run"
echo "Ticker:     $TICKER"
echo "Model:      meta-llama/llama-3.3-70b-instruct (via OpenRouter)"
echo "Train:      $TRAIN_START → $TRAIN_END"
echo "PKL:        $PKL_PATH"
echo "=========================================="

echo "Starting 1-week training run..."
uv run python run.py sim \
    -mdp "$PKL_PATH" \
    -st "$TRAIN_START" \
    -et "$TRAIN_END" \
    -rm train \
    -cp "$CONFIG" \
    -ckp "$TRAIN_CHECKPOINT" \
    -rp "$TRAIN_OUTPUT"

EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Training completed successfully"
else
    echo "✗ Training failed with exit code $EXIT_CODE"
    exit $EXIT_CODE
fi
