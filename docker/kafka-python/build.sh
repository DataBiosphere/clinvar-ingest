#!/usr/bin/env bash
set -euo pipefail

declare -r VERSION=1.0.0

docker build -t us.gcr.io/broad-dsp-gcr-public/monster-kafka-python:VERSION .
docker push us.gcr.io/broad-dsp-gcr-public/monster-kafka-python:VERSION
