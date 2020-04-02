from google.auth import compute_engine
import google.auth.transport.requests
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
import os

base_url = os.environ["API_URL"]
dataset_id = os.environ["DATASET_ID"]
profile_id = os.environ["PROFILE_ID"]
source_path = os.environ["SOURCE_PATH"]
target_path = os.environ["TARGET_PATH"]

request = google.auth.transport.requests.Request()
credentials = compute_engine.IDTokenCredentials(request, base_url, use_metadata_identity_endpoint=True)
authed_session = AuthorizedSession(credentials)

headers = {"accept": "application/json",
           "Content-Type": "application/json"}

def ingest_file(dataset_id: str, **kwargs):
    response = authed_session.post(f"{base_url}/api/repository/v1/datasets/{dataset_id}/files", json=kwargs, headers=headers)
    if response.ok:
        return response.json()["id"]
    else:
        raise HTTPError(f"Bad response, got code of: {response.status_code}")

# print the job id to std out
print(ingest_file(dataset_id, profileId=profile_id, source_path=source_path, target_path=target_path))
