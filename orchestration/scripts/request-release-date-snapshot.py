import google.auth
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
import os
import random
import re
import sys

credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])

base_url = os.environ["API_URL"]
dataset_name = os.environ["DATASET_NAME"]
profile_id = os.environ["PROFILE_ID"]
environment = os.environ["ENVIRONMENT"]

asset_name = os.environ["ASSET_NAME"]
release_date = os.environ["RELEASE_DATE"]
pipeline_version = os.environ["PIPELINE_VERSION"]

authed_session = AuthorizedSession(credentials)


def submit_snapshot(**kwargs):
  response = authed_session.post(f'{base_url}/api/repository/v1/snapshots', json=kwargs)
  if response.ok:
    return response.json()['id']
  else:
    raise HTTPError(f'Bad response, got code of: {response.status_code}')


cleaned_date = release_date.replace('-', '_')
cleaned_version = re.sub('[^a-z0-9_]', '_', pipeline_version.lower())

snapshot_name = f"clinvar_{cleaned_date}_v{cleaned_version}"
snapshot_description = \
  f"Mirror of NCBI's ClinVar archive as of {release_date} (ingest pipeline version: {pipeline_version})"

snapshot_contents = [{
  'datasetName': dataset_name,
  'mode': 'byAsset',
  'assetSpec': {
    'assetName': asset_name,
    'rootValues': [release_date]
  }
}]

reader_emails = ["clingendevs@firecloud.org"] if environment == "prod" else []

job_id = submit_snapshot(name=snapshot_name, description=snapshot_description, contents=snapshot_contents, profileId=profile_id, readers=reader_emails)
print(job_id)
