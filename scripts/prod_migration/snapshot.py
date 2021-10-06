"""
Creates snapshots in "real" prod for existing snapshots in "fake" prod
"""
import re

from dagster_utils.contrib.data_repo.jobs import poll_job
from dagster_utils.resources.data_repo.jade_data_repo import build_client
from data_repo_client import SnapshotRequestModel, SnapshotRequestAssetModel, SnapshotRequestContentsModel

from common import DATA_REPO_URLS, query_yes_no

NEW_PROFILE_ID = "53cf7db4-1ac5-4a83-b204-95d2fea68ff1"
NEW_DATASET_NAME = "broad_dsp_clinvar"
READER_EMAILS = ["clingendevs@firecloud.org", "monster@firecloud.org"]


def run():
    old_data_repo_client = build_client(DATA_REPO_URLS['prod'])
    new_data_repo_client = build_client(DATA_REPO_URLS['real_prod'])

    all_old_snapshots = old_data_repo_client.enumerate_snapshots(filter="clinvar_", limit=1000)
    all_new_snapshots = new_data_repo_client.enumerate_snapshots(filter="clinvar_", limit=1000)

    real_old_snapshot_names = {snapshot.name for snapshot in all_old_snapshots.items}
    real_new_snapshot_names = {snapshot.name for snapshot in all_new_snapshots.items}
    real_new_snapshot_ids = {snapshot.name: snapshot.id for snapshot in all_new_snapshots.items}

    for snapshot_name in sorted(real_old_snapshot_names, reverse=True):
        result = re.search(r"clinvar_(\d{4}_\d{2}_\d{2})_v(.*)", snapshot_name)

        release_date = result.group(1)
        cleaned_release_date = release_date.replace('_', '-')
        pipeline_version = result.group(2)
        cleaned_pipeline_version = pipeline_version.replace('_', '.')

        snapshot_name = f"clinvar_{release_date}_v{pipeline_version}"
        snapshot_description = \
            f"Mirror of NCBI's ClinVar archive as of {cleaned_release_date} (ingest pipeline version: {cleaned_pipeline_version})"

        if snapshot_name not in real_new_snapshot_names:
            print(f"❌ Should create snapshot {snapshot_name}")
            snapshot_request = SnapshotRequestModel(
                name=snapshot_name,
                profile_id=NEW_PROFILE_ID,
                description=snapshot_description,
                contents=[SnapshotRequestContentsModel(
                    dataset_name=NEW_DATASET_NAME,
                    mode="byAsset",
                    asset_spec=SnapshotRequestAssetModel(asset_name="clinvar_release",
                                                         root_values=[cleaned_release_date]))],
                readers=READER_EMAILS
            )
            response = new_data_repo_client.create_snapshot(snapshot=snapshot_request)
            print("polling on JOB ID = " + response.id)
            poll_job(response.id, 3600, 2, new_data_repo_client)
        else:
            print(f"✅ snapshot created [name={snapshot_name},\tid={real_new_snapshot_ids[snapshot_name]}")


if __name__ == '__main__':
    run()
