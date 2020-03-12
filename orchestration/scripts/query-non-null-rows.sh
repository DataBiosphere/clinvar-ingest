set -euo pipefail

# Copy-pasted from StackOverflow.
function join_by { local d=$1; shift; echo -n "$1"; shift; printf "%s" "${@/#/$d}"; }

# Point to BQ metadata we expect to be present on disk.
declare -r TABLE_DIR=/bq-metadata/${TABLE}
declare -r PK_COLS=$(cat ${TABLE_DIR}/primary-keys)

# Build the WHERE clause of the SQL query.
declare -a COMPARISONS=()
for c in ${PK_COLS//,/ }; do
  COMPARISONS+=("${c} IS NOT NULL")
done
declare -r FULL_DIFF=$(join_by ' AND ' "${COMPARISONS[@]}")

# Pull everything but the row ID from rows with non-null primary keys.
# Store the results in another table because you can't directly export
# the results of a query to GCS.
declare -r TARGET_TABLE=${PROJECT}:${DATASET}.${TABLE}_values_${OUTPUT_SUFFIX}

bq --location=US --project_id=${PROJECT} --synchronous_mode=true --headless=true --format=none query \
  --use_legacy_sql=false \
  --destination_table=${TARGET_TABLE} \
  "SELECT * EXCEPT (datarepo_row_id)
   FROM \`${PROJECT}.${DATASET}.${INPUT_TABLE}\`
   WHERE ${FULL_DIFF}"

# Echo the output table name so Argo can slurp it into a parameter.
echo ${TARGET_TABLE}