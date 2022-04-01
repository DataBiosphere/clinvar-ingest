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
declare -r TABLE="\`${JADE_PROJECT}.${JADE_DATASET}.xml_archive\`"


${BQ_QUERY[@]} "SELECT ${PROPERTY} FROM ${TABLE} WHERE release_date = '${RELEASE_DATE}'"
