#!/usr/bin/env bash
set -euo pipefail

docker build -t kafka-python .
docker tag kafka-python us.gcr.io/broad-dsp-gcr-public/monster-kafka-python
docker push us.gcr.io/broad-dsp-gcr-public/monster-kafka-python
