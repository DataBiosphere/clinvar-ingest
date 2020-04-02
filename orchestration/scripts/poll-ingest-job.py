from google.auth import compute_engine
import google.auth.transport.requests
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
import polling
import os
import sys

base_url = os.environ["API_URL"]
job_id = os.environ["JOB_ID"]
timeout = os.environ["TIMEOUT"]

request = google.auth.transport.requests.Request()
credentials = compute_engine.IDTokenCredentials(request, base_url, use_metadata_identity_endpoint=True)
authed_session = AuthorizedSession(credentials)


def check_job_status(job_id: str):
    response = authed_session.get(f"{base_url}/api/repository/v1/jobs/{job_id}")
    if response.ok:
        return response.json()["job_status"]
    else:
        raise HTTPError("Bad response, got code of: {}".format(response.status_code))


def is_done(job_id: str):
    return check_job_status(job_id) == "succeeded"

try:
    polling.poll(lambda: is_done(job_id), step=10, timeout=int(timeout))
    print("true")
except polling.TimeoutException as te:
    while not te.values.empty():
        # Print all of the values that did not meet the exception
        print(te.values.get(), file=sys.stderr)
