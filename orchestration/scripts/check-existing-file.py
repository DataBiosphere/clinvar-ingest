from google.auth import compute_engine
import google.auth.transport.requests
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
import os

base_url = os.environ["API_URL"]
dataset_id = os.environ["DATASET_ID"]
target_path = os.environ["TARGET_PATH"]

request = google.auth.transport.requests.Request()
credentials = compute_engine.IDTokenCredentials(request, base_url, use_metadata_identity_endpoint=True)
authed_session = AuthorizedSession(credentials)


def check_file_existence(target_path: str):
    response = authed_session.get(f"{base_url}/api/repository/v1/datasets/{dataset_id}/filesystem/objects",
                                  params={"path": target_path})
    if response.status_code == 200:
        return "true"
    elif response.status_code == 404:
        return "false"
    else:
        raise HTTPError(f"Unexpected response, got code of: {response.status_code}")

print(check_file_existence(target_path))
