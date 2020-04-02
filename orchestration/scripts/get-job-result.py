from google.auth import compute_engine
import google.auth.transport.requests
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
import os

base_url = os.environ["API_URL"]
job_id = os.environ["JOB_ID"]
# this is the field to pull; for a file ingest, should be "fileId"
result_field = os.environ["RESULT_FIELD"]

request = google.auth.transport.requests.Request()
credentials = compute_engine.IDTokenCredentials(request, base_url, use_metadata_identity_endpoint=True)
authed_session = AuthorizedSession(credentials)


def get_job_result(job_id: str):
    response = authed_session.get(f"{base_url}/api/repository/v1/jobs/{job_id}/result")
    if response.ok:
        return response.json()[result_field]
    else:
        raise HTTPError(f"Bad response, got code of: {response.status_code}")

print(get_job_result(job_id))
