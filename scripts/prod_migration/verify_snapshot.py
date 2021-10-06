"""
Diffs two clinvar snapshots, joining on the primary keys as defined in schema.json
"""
import argparse
from dataclasses import dataclass
from google.cloud.bigquery import Client

from dagster_utils.resources.data_repo.jade_data_repo import build_client
from data_repo_client import RepositoryApi

from common import CLINVAR_TABLES, DATA_REPO_URLS

TABLE_PKS = {
    "gene_association": ["variation_id", "gene_id", "release_date"],
    "processing_history": ["release_date"],
    "trait_mapping": ["clinical_assertion_id", "trait_type", "mapping_type", "mapping_ref", "mapping_value",
                      "release_date"],
    "xml_archive": ["release_date"]
}
DEFAULT_PKS = ["id", "release_date"]


@dataclass
class Snapshot:
    snapshot_id: str
    snapshot_name: str
    snapshot_bq_project: str
    snapshot_region: str


def retrieve_snapshot(snapshot_name: str, client: RepositoryApi) -> Snapshot:
    snapshots = client.enumerate_snapshots(filter=snapshot_name)
    if len(snapshots.items) > 1:
        raise Exception(f"More than one instance of snapshot found [name={snapshot_name}]")

    if len(snapshots.items) == 0:
        raise Exception(f"No snapshot found [name={snapshot_name}")

    bq_location = None
    for storage_item in snapshots.items[0].storage:
        if storage_item.cloud_resource == 'bigquery':
            bq_location = storage_item.region

    if bq_location is None:
        raise Exception(f"No region found for snapshot [name={snapshot_name}]")

    snapshot = client.retrieve_snapshot(id=snapshots.items[0].id)
    return Snapshot(snapshots.items[0].id, snapshot_name, snapshot.data_project, bq_location)


def diff_snapshots(
        old_snapshot: Snapshot,
        new_snapshot: Snapshot,
        client: Client
):
    for table_name in CLINVAR_TABLES:
        pks = TABLE_PKS.get(table_name, DEFAULT_PKS)

        join_clause = " AND ".join([f"n.{pk} = o.{pk}" for pk in pks])
        filter = " OR ".join([f"n.{pk} IS NULL OR o.{pk} IS NULL" for pk in pks])

        except_fields = ["datarepo_row_id"]
        if table_name == 'xml_archive':
            except_fields.append('archive_path')
        except_clause = ",".join(except_fields)

        query = f"""
        with old_snap as (
            SELECT * except ({except_clause})
            FROM `{old_snapshot.snapshot_bq_project}.{old_snapshot.snapshot_name}.{table_name}`
        ),
         new_snap as (
             select * except ({except_clause})
             FROM `{new_snapshot.snapshot_bq_project}.{new_snapshot.snapshot_name}.{table_name}`
         )
        select COUNT(*) as cnt
        from old_snap o
                 FULL OUTER JOIN new_snap n
                                 ON {join_clause}
        WHERE {filter}
           OR (
            TO_JSON_STRING(o) != TO_JSON_STRING(n)
            )
        LIMIT 1;
        """

        print(f"Running diff [table={table_name}]")
        result = client.query(query, location=old_snapshot.snapshot_region, project=old_snapshot.snapshot_bq_project).result()
        rows = [row for row in result]
        cnt = rows[0]['cnt']
        if cnt > 0:
            print(f"❌Found mismatched rows in table [count={cnt}, table={table_name}]")
            print(query)

        print(f"✅ No diff [table={table_name}]")


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--snapshot-name", required=True)
    args = parser.parse_args()

    old_data_repo_client = build_client(DATA_REPO_URLS['prod'])
    new_data_repo_client = build_client(DATA_REPO_URLS['real_prod'])

    new_snapshot = retrieve_snapshot(args.snapshot_name, new_data_repo_client)
    old_snapshot = retrieve_snapshot(args.snapshot_name, old_data_repo_client)

    diff_snapshots(old_snapshot, new_snapshot, Client())


if __name__ == '__main__':
    run()
