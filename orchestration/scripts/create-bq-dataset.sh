set -euo pipefail

# Create a dataset, default expiration of 7 days
declare -ra BQ_CREATE=(
  bq
  --location=US
  mk
  --dataset
  "--description=${DESCRIPTION}"
  --default_table_expiration=${EXPIRATION}
  ${PROJECT}:${DATASET}
)
1>&2 "${BQ_CREATE[@]}"
