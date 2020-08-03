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

${BQ_QUERY[@]} "SELECT COUNT(1) FROM ${TABLE} WHERE release_date = DATE_SUB('${RELEASE_DATE}', INTERVAL 1 DAY) AND pipeline_version = '${VERSION}' LIMIT 1" | tail -n 1
