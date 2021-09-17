import csv

from data_repo_client import RepositoryApi
from prod_migration.common import  get_api_client
import json



def run():
    client: RepositoryApi = get_api_client()
    prod_dataset_id = "dfbd0c7e-088b-45ab-a161-7b79aa28d872"

    with open("clinvar_files.csv") as f:
        reader = csv.reader(f)
        for row in reader:
            release_date, file_id = row
            response = client.lookup_file_by_id(id=prod_dataset_id, fileid=file_id)
            control_item = {
                "source_path": response.file_detail.access_url,
                "target_path": f"/{release_date}.xml.gz"
            }
            print(json.dumps(control_item))



if __name__ == '__main__':
    run()