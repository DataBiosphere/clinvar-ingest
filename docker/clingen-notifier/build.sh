#!/usr/bin/env bash
set -euo pipefail

declare -r VERSION=1.0.0

docker build --build-arg GCS_CLIENT_VERSION=1.28.1 --build-arg KAFKA_CLIENT_VERSION=1.4.2 -t us.gcr.io/broad-dsp-gcr-public/clingen-notifier:${VERSION} .
docker push us.gcr.io/broad-dsp-gcr-public/clingen-notifier:${VERSION}
