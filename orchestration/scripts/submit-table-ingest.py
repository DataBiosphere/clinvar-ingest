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


def ingest_table(dataset_id: str, **kwargs):
    response = authed_session.post(f"{base_url}/api/repository/v1/datasets/{dataset_id}/ingest", json=kwargs)
    if response.ok:
        return response.json()["id"]
    else:
        raise HTTPError(f"Bad response, got code of: {response.status_code}")

# print the job id to std out
print(ingest_table(dataset_id,
                   format="json",
                   ignore_unknown_values=False,
                   max_bad_records=0,
                   path=f"{source_path}/*",
                   table=table_name))
