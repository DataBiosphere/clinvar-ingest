#!/usr/bin/env bash
set -euo pipefail

declare -r VERSION=0.1.0

docker build -t us.gcr.io/broad-dsp-gcr-public/monster-check-snapshot:${VERSION} .
docker push us.gcr.io/broad-dsp-gcr-public/monster-check-snapshot:${VERSION}
