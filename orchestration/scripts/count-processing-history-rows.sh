set -euo pipefail

declare -ra BQ_QUERY=(
  bq
  --apilog=stdout
  --location=US
  --project_id=${PROJECT}
  --synchronous_mode=true
  --headless=true
  --format=csv
  query
  --use_legacy_sql=false
)
declare -r TABLE="\`${JADE_PROJECT}.${JADE_DATASET}.processing_history\`"

gcloud auth list
${BQ_QUERY[@]} "SELECT COUNT(1) FROM ${TABLE} WHERE release_date = '${RELEASE_DATE}' AND pipeline_version = '${VERSION}'" | tail -n 20
