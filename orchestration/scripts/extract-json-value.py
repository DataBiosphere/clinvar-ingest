#!/usr/bin/env python
import argparse
import json

# Argument parsing
parser = argparse.ArgumentParser()
parser.add_argument('--file', type=str, required=True, help='The file where the JSON is stored')
parser.add_argument('--key', type=str, required=True, help='The key used to pull a value from the JSON data')
args = parser.parse_args()

# Opens the provided json-list file and stores to a dict
with open(args.file) as f:
    json_data = json.load(f)

# Uses the provided key to pull the JSON value, printing to stdout for Argo to slurp
print(json_data[args.key])
