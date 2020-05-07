set -euo pipefail

# Copy-pasted from StackOverflow.
function join_by { local d=$1; shift; echo -n "$1"; shift; printf "%s" "${@/#/$d}"; }

# Point to BQ metadata we expect to be present on disk.
declare -r TABLE_DIR=/bq-metadata/${TABLE}
declare -r PK_COLS=$(cat ${TABLE_DIR}/primary-keys)

# Build the WHERE clause of the SQL query.
declare -a DATAREPO_COLUMNS=()
declare -a COMPARISONS=()

# Building one array for columns to select and one for selection criteria
for c in ${PK_COLS//,/ }; do
  DATAREPO_COLUMNS+=("datarepo_${c} as ${c}")
  # where prefixed version is not null and nonprefixed version is null
  COMPARISONS+=("datarepo_${c} is not null and ${c} is null")
done

declare -r FULL_DIFF=$(join_by ' AND ' "${COMPARISONS[@]}")
declare -r REPO_KEYS=$(join_by ', ' "${DATAREPO_COLUMNS[@]}")

declare -r TARGET_TABLE=${TABLE}_clingen_deletes

# Pull all datarepo_ prefixed PK cols where non prefixed version is not null
# make sure to alias them back to nonprefixed names when they are pulled out
# need separate arrays of select list and comparison list
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
1>&2  ${BQ_QUERY[@]} "SELECT ${REPO_KEYS}
  FROM \`${PROJECT}.${DATASET}.${INPUT_TABLE}\`
  WHERE ${FULL_DIFF}"

# Echo the output table name so Argo can slurp it into a parameter.
echo ${TARGET_TABLE}
