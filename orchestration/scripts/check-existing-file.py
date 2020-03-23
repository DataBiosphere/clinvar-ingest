import google.auth
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError

credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])

authed_session = AuthorizedSession(credentials)
base_url = os.environ["API_URL"]
dataset_id = os.environ["DATASET_ID"]
target_path = os.environ["TARGET_PATH"]
headers = {"accept": "application/json"}


def check_file_existence(target_path: str):
    response = authed_session.get(base_url + f"datasets/{dataset_id}/filesystem/objects",
                                  params={"path": target_path},
                                  headers=headers)
    if response.ok:
        return response.status_code
    else:
        raise HTTPError(f"Bad response, got code of: {response.status_code}")

print(check_file_existence(target_path))
