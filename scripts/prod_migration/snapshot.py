from datetime import datetime
import itertools
from data_repo_client import RepositoryApi, SnapshotModel
from google.cloud import bigquery
from google.cloud import storage
from prod_migration.common import CLINVAR_TABLES, get_api_client


def get_snapshot_id(snapshot_name: str, data_repo_client: RepositoryApi) -> str:
    response = data_repo_client.enumerate_snapshots(filter=snapshot_name)
    if response.filtered_total != 1:
        raise ValueError("Error")

    return response.items[0].id


def get_snapshot(snapshot_id: str, data_repo_client: RepositoryApi) -> SnapshotModel:
    response = data_repo_client.retrieve_snapshot(id=snapshot_id)
    return response


def get_ids_for_table(snapshot: SnapshotModel, table_name: str, extraction_path: str, bq_client: bigquery.Client, storage_client: storage.Client):
    project_id = snapshot.data_project
    dataset_name = snapshot.name

    print("Clearing out existing data...")
    bucket = storage.Bucket(storage_client, "broad-dsp-monster-clinvar-snapshot-migration")
    all_blobs = bucket.list_blobs(prefix=f"dump/{table_name}")
    for blob in all_blobs:
        blob.delete()

    print(f"Dumping row ids for {table_name}...")
    query = f"""
     EXPORT DATA OPTIONS(
        uri='{extraction_path}/{table_name}/raw/{table_name}-*.json',
        format='CSV',
        overwrite=true
    ) AS
    SELECT DISTINCT * FROM (SELECT datarepo_row_id FROM `{project_id}.{dataset_name}.{table_name}`)
    """

    bq_client.query(query, project=project_id).result()
    blobs = [blob for blob in bucket.list_blobs(prefix=f"dump/{table_name}/raw")]

    chunked = list(itertools.zip_longest(*[iter(blobs)] * 32))

    for i, chunk in enumerate(chunked):
        filtered = [item for item in chunk if item]
        destination_blob = bucket.blob(f"dump/{table_name}/chunked/merged-{i}.json")
        destination_blob.compose(sources=filtered)

    chunk_blobs = [blob for blob in bucket.list_blobs(prefix=f"dump/{table_name}/chunked")]
    destination_merged = bucket.blob(f"dump/{table_name}/merged/merged.json")
    destination_merged.compose(sources=chunk_blobs)

    #destination_blob.compose(sources=blobs)
    print(f"done")


def run(snapshot_name: str):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    bq_client = bigquery.Client()
    storage_client = storage.Client()


    data_repo_client = get_api_client()

    snapshot_id = get_snapshot_id(snapshot_name, data_repo_client)
    snapshot = get_snapshot(snapshot_id, data_repo_client)
    asset_name = 'clinvar_release'
    mode = 'byAsset'


    import pdb; pdb.set_trace()
    # for table_name in CLINVAR_TABLES:
    #     get_ids_for_table(
    #         snapshot,
    #         table_name,
    #         f"gs://broad-dsp-monster-clinvar-snapshot-migration/dump",
    #         bq_client,
    #         storage_client
    #     )


if __name__ == '__main__':
    run("clinvar_2021_07_10_v1_3_9")
