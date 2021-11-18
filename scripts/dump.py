from google.cloud.bigquery.client import Client
import argparse



def run(data_project):
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

    for table_name in table_names:
        query = f"""
        EXPORT DATA OPTIONS(
          uri="gs://broad-dsp-monster-clinvar-dspdc-1950/{data_project}/{table_name}/*",
          format="JSON",
          overwrite=true,
          compression="gzip"
        ) AS SELECT * FROM `{data_project}.datarepo_broad_dsp_clinvar.{table_name}`;
        """

        print(f"Dumping {data_project}.{table_name}...")
        client.query(query).result()




if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--data_project", required=True)

    args = parser.parse_args()
    run(args.data_project)