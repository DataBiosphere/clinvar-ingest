from datetime import datetime
from multiprocessing import Pool
from typing import NamedTuple

from dagster_utils.contrib.data_repo.jobs import poll_job
from google.cloud.bigquery import Client

from common import CLINVAR_TABLES, get_api_client, DATA_REPO_URLS


def retrieve_snapshot_data_project(id_name: (str, str)) -> None:
    data_repo_client = get_api_client(DATA_REPO_URLS['prod'])

    response = data_repo_client.retrieve_snapshot(id=id_name[0])
    print(f"{id_name[1]}\t{id_name[0]}\t{response.data_project}")


def retrieve_snapshots():
    data_repo_client = get_api_client()
    response = data_repo_client.enumerate_snapshots(filter="clinvar", limit=1000)

    snapshot_id_names = [(snapshot.id, snapshot.name) for snapshot in response.items]
    with Pool(10) as p:
        p.map(retrieve_snapshot_data_project, snapshot_id_names)


class TableNameWithTimestamp(NamedTuple):
    table_name: str
    timestamp: str


def dump_table(table_name_with_timestamp: TableNameWithTimestamp):
    bq_client = Client()
    timestamp = table_name_with_timestamp.timestamp
    table_name = table_name_with_timestamp.table_name
    extraction_path = f"gs://broad-dsp-monster-clinvar-20210910-dump/{timestamp}"
    bq_project_id = 'broad-datarepo-terra-prod-cgen'
    dataset_name = 'datarepo_broad_dsp_clinvar'

    except_clause = "datarepo_row_id"
    if table_name == "xml_archive":
        except_clause = "datarepo_row_id, archive_path"

    query = f"""
        EXPORT DATA OPTIONS(
            uri='{extraction_path}/{table_name}/*',
            format='JSON',
            overwrite=true
        ) AS
        SELECT * EXCEPT ({except_clause}) FROM `{bq_project_id}.{dataset_name}.{table_name}` WHERE release_date = '2021-09-12'
    """

    print(f"Dumping table {table_name} to {extraction_path}/{table_name}...")
    try:
        bq_client.query(query, project=bq_project_id).result()
    except Exception as e:
        print(e)


def dump_clinvar_tables():
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    with Pool(10) as p:
        p.map(dump_table, [TableNameWithTimestamp(table_name, timestamp) for table_name in CLINVAR_TABLES])

    p.join()

    return timestamp


class TableLoadRequest(NamedTuple):
    table_name: str
    file_path: str


def load_table(table_load_request: TableLoadRequest):
    try:
        data_repo_client = get_api_client(DATA_REPO_URLS["real_prod"])
        payload = {
            "format": "json",
            "ignore_unknown_values": "false",
            "max_bad_records": 0,
            "path": f"{table_load_request.file_path}/{table_load_request.table_name}/*",
            "table": table_load_request.table_name
        }
        print(f"Ingesting data for {table_load_request.table_name} ...")
        job_response = data_repo_client.ingest_dataset(
            id="29844445-5005-4672-a2f1-f238de23dc20",
            ingest=payload
        )
        print(f"Polling on job_id = {job_response.id} for table_name = {table_load_request.table_name}")
        poll_job(job_response.id, 1800, 2, data_repo_client)
        print(f"Done polling on job_id = {job_response.id} for table_name = {table_load_request.table_name}")
    except Exception as e:
        print(e)


def load_clinvar_tables(timestamp):
    table_names_with_paths = [
        TableLoadRequest(table_name, f"gs://broad-dsp-monster-clinvar-20210910-dump/{timestamp}")
        for table_name in CLINVAR_TABLES
    ]

    with Pool(10) as p:
        p.map(load_table, table_names_with_paths)
    #
    # for table in table_names_with_paths:
    #     load_table(table)


def run():
    #  timestamp = dump_clinvar_tables()
    timestamp = 20210923122228


# load_clinvar_tables(timestamp)


if __name__ == '__main__':
    run()
