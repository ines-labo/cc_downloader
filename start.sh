#!/bin/bash
python3 src/download_weights.py
exec python3 src/openwarc_parallel.py --config ./src/config.yaml