set -euo pipefail

# Pull just the non-null row IDs out of a table. We need the results in
# a table because you can't directly export the results of a query to GCS.
declare -r TARGET_TABLE=${OUTPUT_PREFIX}_rowids_${OUTPUT_SUFFIX}

1>&2 bq --location=US --project_id=${PROJECT} --synchronous_mode=true --headless=true --format=none query \
  --use_legacy_sql=false --replace=true \
  --destination_table=${PROJECT}:${DATASET}.${TARGET_TABLE} \
  "SELECT datarepo_row_id
   FROM \`${PROJECT}.${DATASET}.${INPUT_TABLE}\`
   WHERE datarepo_row_id IS NOT NULL"

# Echo the output table name so Argo can slurp it into a parameter.
echo ${TARGET_TABLE}
