set -euo pipefail

declare -ra BQ_QUERY=(
  bq
  --location=US
  --project_id=${JADE_PROJECT}
  --synchronous_mode=true
  --headless=true
  --format=csv
  query
  --use_legacy_sql=false
)
declare -r TABLE="\`${JADE_PROJECT}.${JADE_DATASET}.processing_history\`"

${BQ_QUERY[@]} "SELECT COUNT(1) FROM (SELECT release_date FROM ${TABLE} WHERE release_date < '${RELEASE_DATE}' ORDER BY release_date DESC LIMIT 1)" | tail -n 1
