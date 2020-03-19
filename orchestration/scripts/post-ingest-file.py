import google.auth
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
import os

credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])

authed_session = AuthorizedSession(credentials)
base_url = os.environ["API_URL"]

headers = {"accept": "application/json",
           "Content-Type": "application/json"}

def ingest_file(dataset_id: str, **kwargs):
    response = authed_session.post(base_url + "datasets/{}/files".format(dataset_id), json=kwargs, headers=headers)
    if response.ok:
        return response.json()["id"]
    else:
        raise HTTPError("Bad response, got code of: {}".format(response.status_code))

# print the job id to std out
print(ingest_file(os.environ["DATASET_ID"], profileId=os.environ["PROFILE_ID"], source_path=os.environ["SOURCE_PATH"], target_path=os.environ["TARGET_PATH"]))
