#!/usr/bin/env bash
set -euo pipefail

declare -r VERSION=1.0.1

docker build --build-arg GOOGLE_AUTH_VERSION=1.13.1 --build-arg REQUESTS_VERSION=2.23.0 --build-arg POLLING_VERSION=0.3.0 -t us.gcr.io/broad-dsp-gcr-public/monster-auth-req-py:${VERSION} .
docker push us.gcr.io/broad-dsp-gcr-public/monster-auth-req-py:${VERSION}
