#!/bin/bash
python3 src/download_weights.py
exec python3 src/openwarc_parallel.py --working_dir=./src --dataset_dir=./dataset/commoncrawl_parallel/shards --num_proc=4