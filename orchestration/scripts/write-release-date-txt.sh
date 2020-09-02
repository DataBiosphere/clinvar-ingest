set -euo pipefail

echo ${RELEASE_DATE} > release_date.txt

gsutil cp release_date.txt gs://${BUCKET}/${FILE_PATH}/release_date.txt