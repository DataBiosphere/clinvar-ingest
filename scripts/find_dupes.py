from google.cloud.bigquery.client import Client
import argparse


def run(data_project: str):
    client = Client()
    table_names = [
        'clinical_assertion',
        'clinical_assertion_observation',
        'clinical_assertion_trait',
        'clinical_assertion_trait_set',
        'clinical_assertion_variation',
        'gene',
        'gene_association',
        'rcv_accession',
        'submission',
        'submitter',
        'trait',
        'trait_mapping',
        'trait_set',
        'variation',
    ]

    data_project = "datarepo-4c7e3eea"
    #data_project = "broad-datarepo-terra-prod-cgen"
    for table in table_names:
        pks = "id, release_date"
        if table == 'gene_association':
            pks = "gene_id, variation_id, release_date"
        elif table == "trait_mapping":
            pks = "clinical_assertion_id, trait_type, mapping_type, mapping_ref, mapping_value, release_date"
        query = f"""
        with base as (
          SELECT {pks}, count(*) FROM `{data_project}.datarepo_broad_dsp_clinvar.{table}` 
          group by {pks}
          having count(*) > 1
        )
        select distinct release_date from base;
        """
        results = client.query(query).result()
        [print(f"{table}, {row}") for row in results]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--data_project", required=True)

    args = parser.parse_args()
    run(args.data_project)