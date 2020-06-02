#!/usr/bin/env bash
set -euo pipefail

declare -r VERSION=1.1.0

docker build --build-arg KAFKA_CLIENT_VERSION=1.4.2 -t us.gcr.io/broad-dsp-gcr-public/monster-kafka-python:${VERSION} .
docker push us.gcr.io/broad-dsp-gcr-public/monster-kafka-python:${VERSION}
