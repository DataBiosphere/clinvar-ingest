from google.cloud import bigquery
from typing import Dict
from enum import Enum
import os

# "broad-datarepo-terra-prod-cgen"
repo_data_project = os.getenv("REPO_DATA_PROJECT")
# "datarepo_broad_dsp_clinvar"
repo_dataset_name = os.getenv("REPO_DATASET_NAME")
# release_date = "2020-06-02"
release_date = os.getenv("RELEASE_DATE")
# prev_date = "2020-05-06"
prev_date = os.getenv("PREV_DATE")
# "broad-dsp-monster-clingen-prod"
dest_data_project = os.getenv("DEST_DATA_PROJECT")
# dest_dataset_name = "raaidTest1"
dest_dataset_name = os.getenv("DEST_DATASET_NAME")
# dest_bucket_name = "broad-dsp-monster-clingen-dev-ingest-results"
dest_bucket_name = os.getenv("DEST_BUCKET_NAME")
# path of table to extract to
dest_path = os.getenv("DEST_PATH")
# the column name with the date/time, so "release_date" for clinvar
version_col = os.getenv("VERSION_COL_NAME")
# CREATE, DELETE, or UPDATE
diff_type = os.getenv("DIFF_TYPE")
# table name that should exist in the dataset
table_name = os.getenv("TABLE_NAME")
# make sure to pass in GOOGLE_APPLICATION_CREDENTIALS

repo_client = bigquery.Client(project=repo_data_project) if repo_data_project else None
dest_client = bigquery.Client(project=dest_data_project) if dest_data_project else None


class DiffType(Enum):
    CREATE = "created"
    DELETE = "deleted"
    UPDATE = "updated"


def create_dataset(dataset_name: str = dest_dataset_name, client: bigquery.Client = dest_client) -> str:
    # should use the destination project client client
    dataset_id = f"{client.project}.{dataset_name}"

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    # 1 week in milliseconds
    dataset.default_table_expiration_ms = 7 * 24 * 60 * 60 * 1000
    dataset.location = "US"

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset, timeout=30)
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    return dataset_name


def get_non_pk_cols(table_name: str, repo_data_project: str, repo_dataset_name: str, client: bigquery.Client) -> Dict[
    str, str]:
    # should use the jade client
    query = f"""
    SELECT column_name, data_type FROM `{repo_data_project}.{repo_dataset_name}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = "{table_name}" AND column_name NOT IN ("id", "release_date", "datarepo_row_id")
    """

    query_job = client.query(query)

    return {row[0]: row[1] for row in query_job}


def get_all_rows(table_name: str = table_name, dest_data_project: str = dest_data_project,
                 dest_dataset_name: str = dest_dataset_name, repo_data_project: str = repo_data_project,
                 repo_dataset_name: str = repo_dataset_name, release_date: str = release_date,
                 version_col: str = version_col, client: bigquery.Client = repo_client):
    query = f"""
    SELECT * EXCEPT (datarepo_row_id, release_date)
    FROM `{repo_data_project}.{repo_dataset_name}.{table_name}`
    WHERE {version_col} = "{release_date}"
    """

    dest_table_name = f"{table_name}_{DiffType.CREATE.value}"
    table_id = f"{dest_data_project}.{dest_dataset_name}.{dest_table_name}"

    job_config = bigquery.QueryJobConfig(destination=table_id)

    query_job = client.query(query, job_config=job_config)
    # wait till job is completed
    client.get_job(
        query_job.job_id, location=query_job.location
    )
    # return the destination table name
    return dest_table_name


def get_created_rows():
    cols = get_non_pk_cols(table_name, repo_data_project, repo_dataset_name, repo_client)
    # use defaults otherwise
    get_rows(DiffType.CREATE, cols=cols)


def get_deleted_rows():
    cols = get_non_pk_cols(table_name, repo_data_project, repo_dataset_name, repo_client)
    get_rows(DiffType.DELETE, cols=cols)


def get_updated_rows():
    cols = get_non_pk_cols(table_name, repo_data_project, repo_dataset_name, repo_client)
    get_rows(DiffType.UPDATE, cols=cols)


def get_rows(diff_type: DiffType, cols: Dict[str, str], table_name: str = table_name,
             dest_data_project: str = dest_data_project, dest_dataset_name: str = dest_dataset_name,
             repo_data_project: str = repo_data_project, repo_dataset_name: str = repo_dataset_name,
             release_date: str = release_date, prev_date: str = prev_date, version_col: str = version_col,
             client: bigquery.Client = repo_client) -> str:
    # should use the jade client
    if diff_type is DiffType.CREATE:
        select_table = "A"
        join = "LEFT JOIN"
        on = "ON A.id = B.id"
        where = "WHERE B.id IS NULL"
    elif diff_type is DiffType.DELETE:
        select_table = "B"
        join = "RIGHT JOIN"
        on = "ON A.id = B.id"
        where = "WHERE A.id IS NULL"
    elif diff_type is DiffType.UPDATE:
        select_table = "A"
        join = "JOIN"
        # for update, we need everything where the ids are the same but ANYTHING at all in the other columns is
        # different. We join on the id, AND we join on inequality between all other columns, so all the other cols
        # are OR-ed together. Some of them are ARRAY cols, so we need to convert them to string before comparing.
        diff_join = " OR ".join([
            f"ARRAY_TO_STRING(A.{col_name}, \" \") != ARRAY_TO_STRING(B.{col_name}, \" \")"
            if "ARRAY" in col_type else f"A.{col_name} != B.{col_name}"
            for col_name, col_type in cols.items()])
        on = f"ON A.id = B.id AND ({diff_join})"
        where = ""
    else:
        raise ValueError("Invalid DiffType for get_rows")

    selected_cols = ", ".join([f"{select_table}.{col}" for col in tuple(cols)])
    query = f"""
    SELECT {select_table}.id, {selected_cols} FROM
    (SELECT * FROM `{repo_data_project}.{repo_dataset_name}.{table_name}` WHERE {version_col} = "{release_date}") AS A
    {join}
    (SELECT * FROM `{repo_data_project}.{repo_dataset_name}.{table_name}` WHERE {version_col} = "{prev_date}") AS B
    {on} {where};
    """

    dest_table_name = f"{table_name}_{diff_type.value}"
    table_id = f"{dest_data_project}.{dest_dataset_name}.{dest_table_name}"

    job_config = bigquery.QueryJobConfig(destination=table_id)

    query_job = client.query(query, job_config=job_config)
    # wait till job is completed
    client.get_job(
        query_job.job_id, location=query_job.location
    )
    # return the destination table name
    return dest_table_name


def extract_rows(table_name: str = table_name, bucket_name: str = dest_bucket_name, path: str = dest_path,
                 diff_type: DiffType = diff_type, dest_data_project: str = dest_data_project,
                 dest_dataset_name: str = dest_dataset_name, client: bigquery.Client = dest_client):
    job_config = bigquery.ExtractJobConfig(printHeader=False, destination_format="NEWLINE_DELIMITED_JSON")

    destination_uri = f"gs://{bucket_name}/{path}/{table_name}/{diff_type.value}/*"
    dataset_ref = bigquery.DatasetReference(dest_data_project, dest_dataset_name)
    table_ref = dataset_ref.table(table_name)

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location="US",
        job_config=job_config
    )  # API request
    return extract_job.result()
