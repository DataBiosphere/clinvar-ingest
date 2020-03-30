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
    response = authed_session.get(f"{base_url}/datasets/{dataset_id}/filesystem/objects",
                                  params={"path": target_path},
                                  headers=headers)
    if response.status_code == 200:
        return "true"
    elif response.status_code == 404:
        return "false"
    else:
        raise HTTPError(f"Unexpected response, got code of: {response.status_code}")

print(check_file_existence(target_path))
