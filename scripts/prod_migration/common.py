import sys

from dagster_utils.resources.data_repo.jade_data_repo import build_client

DATA_REPO_URLS = {
    "dev": "https://jade.datarepo-dev.broadinstitute.org/",
    "prod": "https://jade-terra.datarepo-prod.broadinstitute.org/",
    "real_prod": "https://data.terra.bio/"
}
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




def get_api_client(url):
    return build_client(url)



def query_yes_no(question: str, default: str = "no") -> bool:
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError(f"invalid default answer: '{default}'")

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")

