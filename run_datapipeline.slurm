#!/bin/bash
#SBATCH --job-name=stock_datapipeline_%a
#SBATCH --output=runs/%A_%a/datapipeline_%A_%a.out
#SBATCH --error=runs/%A_%a/datapipeline_%A_%a.err
#SBATCH --time=24:00:00
#SBATCH --partition=long
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=70G
#SBATCH --mail-type=ALL
#SBATCH --mail-user=ivan.lee@u.nus.edu

echo "=========================================="
echo "Job ID: $SLURM_JOB_ID"
echo "Array Task ID: $SLURM_ARRAY_TASK_ID"
echo "Stock: $STOCK"
echo "Date Range: $START_DATE to $END_DATE"
echo "Node: $SLURM_NODELIST"
echo "=========================================="

mkdir -p runs/${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}

echo "Syncing dependencies..."
uv sync

echo "Running Free SEC pipeline for $STOCK..."
uv run data-pipeline/01_Free_SEC_download.py 

echo "Running Google News pipeline for $STOCK..."
uv run python data-pipeline/02_Google_news.py
