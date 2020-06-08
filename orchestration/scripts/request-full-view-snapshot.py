import google.auth
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
import os
import random
import sys

credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])

base_url = os.environ["API_URL"]
dataset_name = os.environ["DATASET_NAME"]
profile_id = os.environ["PROFILE_ID"]
snapshot_name = os.environ["SNAPSHOT_NAME"]

authed_session = AuthorizedSession(credentials)


def submit_snapshot(**kwargs):
  response = authed_session.post(f'{base_url}/api/repository/v1/snapshots', json=kwargs)
  if response.ok:
    return response.json()['id']
  else:
    raise HTTPError(f'Bad response, got code of: {response.status_code}')

snapshot_contents = [{
  'datasetName': dataset_name,
  'mode': 'byFullView'
}]

print(submit_snapshot(contents=snapshot_contents, name=snapshot_name, profileId=profile_id))
