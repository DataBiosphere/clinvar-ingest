set -euo pipefail

# Create a dataset
declare -ra BQ_CREATE=(
  bq
  --location=US
  mk
  --dataset
  ${PROJECT}:${DATASET}
)
1>&2 ${BQ_CREATE[@]}
