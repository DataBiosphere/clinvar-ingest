import google.auth
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
import os

credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])

base_url = os.environ["API_URL"]
dataset_id = os.environ["DATASET_ID"]
profile_id = os.environ["PROFILE_ID"]
source_path = os.environ["SOURCE_PATH"]
target_path = os.environ["TARGET_PATH"]

authed_session = AuthorizedSession(credentials)


def ingest_file(dataset_id: str, **kwargs):
    response = authed_session.post(f"{base_url}/api/repository/v1/datasets/{dataset_id}/files", json=kwargs)
    if response.ok:
        return response.json()["id"]
    else:
        raise HTTPError(f"Bad response, got code of: {response.status_code}")

# print the job id to std out
print(ingest_file(dataset_id, profileId=profile_id, source_path=source_path, target_path=target_path))
