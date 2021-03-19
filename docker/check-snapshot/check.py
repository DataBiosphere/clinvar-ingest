import logging
import os

from data_repo_client import ApiClient, Configuration, RepositoryApi
import google.auth
from google.auth.transport.requests import Request
from google.cloud import bigquery


logging.basicConfig(level=logging.INFO)

ENV = os.getenv("ENV")
google_project = os.getenv("REPO_DATA_PROJECT")
data_repo_host = os.getenv("REPO_HOST")
dataset_name = os.getenv("REPO_DATASET_NAME")
dataset_id = os.getenv("REPO_DATASET_ID")


def default_google_access_token():
    # get token for google-based auth use, assumes application default credentials work for specified environment
    credentials, _ = google.auth.default()
    credentials.refresh(Request())

    return credentials.token


def get_api_client(host: str) -> RepositoryApi:
    # create API client
    config = Configuration(host=host)
    config.access_token = default_google_access_token()
    client = ApiClient(configuration=config)
    client.client_side_validation = False

    return RepositoryApi(api_client=client)



# get latest row from xml archive: SELECT release_date FROM BLAH ORDER BY release_date DESC LIMIT 1
def get_latest_xml_release_date(project: str, dataset: str) -> str:
    bq_client = bigquery.Client(project=project)
    query = f"""
        SELECT release_date
        FROM `{project}.{dataset}.xml_archive`
        ORDER BY release_date DESC LIMIT 1
    """
    results = bq_client.query(query)
    release_date = [row[0] for row in results][0]
    cleaned_date = str(release_date).replace("-", "_")
    return cleaned_date

def check_snapshot_exists(host: str, dataset_id: str, filter: str) -> bool:
    jade_client = get_api_client(host=host)
    r = jade_client.enumerate_snapshots(
        limit=1,sort="created_date", direction="desc", dataset_ids=[dataset_id], filter=filter)
    assert r.total == 1, "No snapshot found for latest release date in xml_archive table"

def run():
    latest_release_date = get_latest_xml_release_date(project=google_project, dataset=dataset_name)
    logging.info(f"Latest release date in XML archive is {latest_release_date}")
    check_snapshot_exists(host=data_repo_host, dataset_id=dataset_id, filter=latest_release_date)

