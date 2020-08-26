set -euo pipefail

declare -ra BQ_QUERY=(
  bq
  --location=US
  --project_id=${PROJECT}
  --synchronous_mode=true
  --headless=true
  --format=csv
  query
  --use_legacy_sql=false
)
declare -r TABLE="\`${JADE_PROJECT}.${JADE_DATASET}.processing_history\`"

${BQ_QUERY[@]} "SELECT release_date FROM ${TABLE} WHERE release_date < '${RELEASE_DATE}' AND pipeline_version = '${VERSION}' ORDER BY release_date DESC LIMIT 1" | tail -n 1
