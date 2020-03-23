import google.auth
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
import polling
import os
import sys

credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])

authed_session = AuthorizedSession(credentials)
base_url = os.environ["API_URL"]
job_id = os.environ["JOB_ID"]
timeout = os.environ["TIMEOUT"]
headers = {"accept": "application/json"}

def check_job_status(job_id: str):
    response = authed_session.get(base_url + "jobs/" + job_id, headers=headers)
    if response.ok:
        return response.json()["job_status"]
    else:
        raise HTTPError("Bad response, got code of: {}".format(response.status_code))


def is_done(job_id: str):
    return check_job_status(job_id) == "succeeded"

try:
    polling.poll(lambda: is_done(job_id), step=10, timeout=int(timeout))
    print(True)
except polling.TimeoutException as te:
    while not te.values.empty():
        # Print all of the values that did not meet the exception
        print(te.values.get(), file=sys.stderr)
