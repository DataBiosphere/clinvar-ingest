set -euo pipefail

# Extract the table's contents into part-files in GCS.
1>&2 bq --location=US --project_id=${PROJECT} --synchronous_mode=true --headless=true --format=none extract \
  --destination_format=${OUTPUT_FORMAT} \
  --print_header=false \
  ${PROJECT}:${DATASET}.${TABLE} \
  gs://${GCS_BUCKET}/${GCS_PREFIX}/*

# Echo the GCS prefix back to Argo, to make plumbing it through as an output easier.
echo ${GCS_PREFIX}
