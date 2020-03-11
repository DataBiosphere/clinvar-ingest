set -euo pipefail

# Copy-pasted from StackOverflow.
function join_by { local d=$1; shift; echo -n "$1"; shift; printf "%s" "${@/#/$d}"; }

declare -r TABLE_DIR=/bq-metadata/${TABLE}
declare -r PK_COLS=$(cat ${TABLE_DIR}/primary-keys)
declare -r COMPARE_COLS=$(cat ${TABLE_DIR}/compare-cols)
declare -a COMPARISONS=()

for c in ${COMPARE_COLS//,/ }; do
  COMPARISONS+=("TO_JSON_STRING(S.${c}) != TO_JSON_STRING(J.${c})")
done

declare -r FULL_DIFF=$(join_by ' OR ' "${COMPARISONS[@]}")

bq --location=US --project_id=${STAGING_PROJECT} --synchronous_mode=true --headless=true --format=none query \
  --use_legacy_sql=false \
  --external_table_definition=${TABLE}::${TABLE_DIR}/schema.json@NEWLINE_DELIMITED_JSON=${GCS_PREFIX}/${TABLE}/* \
  --destination_table=${STAGING_PROJECT}:${STAGING_DATASET}.${TABLE}_${OUTPUT_SUFFIX} \
  "SELECT J.datarepo_row_id, S.*
    FROM ${TABLE} S FULL JOIN \`${JADE_PROJECT}.${JADE_DATASET}.${TABLE}\` J
    USING (${PK_COLS}) WHERE ${FULL_DIFF}"
