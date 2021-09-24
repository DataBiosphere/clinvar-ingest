from google.cloud.bigquery import Client

from common import CLINVAR_TABLES

NEW_PROD_DATASET = "datarepo-4c7e3eea.datarepo_broad_dsp_clinvar"
OLD_PROD_DATASET = "broad-datarepo-terra-prod-cgen.datarepo_broad_dsp_clinvar"


def get_table_row_count(dataset: str, table_name: str, client: Client) -> int:
    query = f"""
    SELECT COUNT(*) as cnt FROM `{dataset}.{table_name}`
    """

    rows = [row for row in client.query(query).result()]
    return rows[0]['cnt']


def run():
    client = Client()
    for table_name in CLINVAR_TABLES:
        print(f"Verifying {table_name}...")
        old_count = get_table_row_count(OLD_PROD_DATASET, table_name, client)
        new_count = get_table_row_count(NEW_PROD_DATASET, table_name, client)
        if old_count != new_count:
            print(f"❌ {table_name}\told_count = {old_count}\tnew_count = {new_count}")
        else:
            print(f"✅ {table_name}\told_count = {old_count}\tnew_count = {new_count}")


if __name__ == '__main__':
    run()
