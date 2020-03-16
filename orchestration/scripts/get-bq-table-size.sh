set -euo pipefail

# Query the size of the target table as CSV, and strip away the header row.
# The single value will be printed to stdout, and slurped by Argo.
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
${BQ_QUERY[@]} "SELECT COUNT(1) FROM \`${PROJECT}.${DATASET}.${TABLE}\`" | tail -n 1
