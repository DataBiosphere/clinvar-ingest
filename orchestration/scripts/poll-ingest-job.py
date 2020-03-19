import google.auth
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import HTTPError
import polling
import os

credentials, project = google.auth.default(scopes=['openid', 'email', 'profile'])

authed_session = AuthorizedSession(credentials)
base_url = os.environ["API_URL"]
headers = {"accept": "application/json"}

def check_job_status(job_id: str):
    response = authed_session.get(base_url + "jobs/" + job_id, headers=headers)
    if response.ok:
        return response.json()["job_status"]
    else:
        raise HTTPError("Bad response, got code of: {}".format(response.status_code))


def is_done(job_id: str):
    return check_job_status(job_id) == "succeeded"

def get_job_result(job_id: str):
    response = authed_session.get(base_url + "jobs/{}/result".format(job_id), headers=headers)
    if response.ok:
        return response.json()["fileId"]
    else:
        raise HTTPError("Bad response, got code of: {}".format(response.status_code))

try:
    polling.poll(lambda: is_done(os.environ["JOB_ID"]), step=10, timeout=300)
except polling.TimeoutException as te:
    while not te.values.empty():
        # Print all of the values that did not meet the exception
        print(te.values.get())

# only gets here if poll eventually gets a "succeeded" status return
print(get_job_result(os.environ["JOB_ID"]))
