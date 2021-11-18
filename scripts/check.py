import json

from collections import defaultdict

cnts = defaultdict(int)

with open("/Users/aherbst/src/clinvar-ingest/debug/kafka_verify/20211114T010000/variation/updated/000000000000") as f:
    lines = f.readlines()
    for line in lines:
        j = json.loads(line)

        id = j["id"]
        cnts[id] += 1

for k, v in cnts.items():
    if v > 1:
        print(f"{k} -> {v}")
