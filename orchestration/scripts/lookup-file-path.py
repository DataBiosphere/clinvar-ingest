import google.auth
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
import os

credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])

base_url = os.environ["API_URL"]
dataset_id = os.environ["DATASET_ID"]
file_id = os.environ["FILE_ID"]

authed_session = AuthorizedSession(credentials)


def get_file_path(id: str):
  response = authed_session.get(f'{base_url}/api/repository/v1/datasets/{dataset_id}/files/{id}')
  if response.status_code != 200:
    raise HTTPError(f'Unexpected response, got code of: {response.status_code}')

  return response.json()['fileDetail']['accessUrl']


print(get_file_path(file_id))
