#!/usr/bin/env bash
set -euo pipefail

declare -r VERSION=1.0.4

docker build -t us.gcr.io/broad-dsp-gcr-public/monster-bq-diff:${VERSION} .
docker push us.gcr.io/broad-dsp-gcr-public/monster-bq-diff:${VERSION}
