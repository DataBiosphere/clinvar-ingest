from dagster_utils.resources.data_repo.jade_data_repo import build_client

PROD_REPO_URL = "https://jade-terra.datarepo-prod.broadinstitute.org/"

CLINVAR_TABLES = [
    'clinical_assertion',
    'clinical_assertion_observation',
    'clinical_assertion_trait',
    'clinical_assertion_trait_set',
    'clinical_assertion_variation',
    'gene',
    'gene_association',
    'processing_history',
    'rcv_accession',
    'submission',
    'submitter',
    'trait',
    'trait_mapping',
    'trait_set',
    'variation',
    'variation_archive',
    'xml_archive'
]




def get_api_client():
    return build_client(PROD_REPO_URL)