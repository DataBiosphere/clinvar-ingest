set -euo pipefail

declare -r TARGET_TABLE=${TABLE}_clingen_${QUERY_TYPE}

# Pull everything but the row ID from rows with non-null primary keys.
# Store the results in another table because you can't directly export
# the results of a query to GCS.
declare -ra BQ_QUERY=(
  bq
  --location=US
  --project_id=${PROJECT}
  --synchronous_mode=true
  --headless=true
  --format=none
  query
  --use_legacy_sql=false
  --replace=true
  --destination_table=${PROJECT}:${DATASET}.${TARGET_TABLE}
)
1>&2  ${BQ_QUERY[@]} "SELECT * EXCEPT (${REPO_KEYS})
  FROM \`${PROJECT}.${DATASET}.${INPUT_TABLE}\`
  WHERE ${FULL_DIFF}"

# Echo the output table name so Argo can slurp it into a parameter.
echo ${TARGET_TABLE}
