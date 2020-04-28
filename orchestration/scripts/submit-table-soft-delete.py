import google.auth
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
import os

credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])

base_url = os.environ["API_URL"]
dataset_id = os.environ["DATASET_ID"]
source_path = os.environ["SOURCE_PATH"]
table_name = os.environ["TABLE_NAME"]

authed_session = AuthorizedSession(credentials)


def soft_delete_table(dataset_id: str, **kwargs):
  response = authed_session.post(f"{base_url}/api/repository/v1/datasets/{dataset_id}/deletes", json=kwargs)
  if response.ok:
    return response.json()["id"]
  else:
    raise HTTPError(f"Bad response, got code of: {response.status_code}")

table_spec = {
  "gcsFileSpec": {
    "fileType": "csv",
    "path": f"{source_path}/*"
  },
  "tableName": table_name
}
# Print the job ID to std out.
print(soft_delete_table(dataset_id, deleteType="soft", specType="gcsFile", tables=[table_spec]))
