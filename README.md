# ClinVar Ingest
Batch ETL pipeline to mirror ClinVar releases into the Jade Data Repository.

### How this ingest process works

1. [Argo Workflow](https://github.com/argoproj/argo)
    1. Download the latest ClinVar release from the 
    [ClinVar FTP server](ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/clinvar_variation/) 
    to a temporary local volume
    2. Run the compressed download file through our command line 
    [XML -> JSON tool](https://github.com/broadinstitute/monster-xml-to-json-list) 
    and store the output to a temporary local volume
    3. Upload both the raw XML data and the JSON data to GCS using [gsutil](https://github.com/GoogleCloudPlatform/gsutil)
2. [Dataflow](https://cloud.google.com/dataflow/)
    1. Transform the JSON input data from GCS into the specified output schema
    2. Store output part files in GCS
3. Jade API: Use the Jade API to perform a tabular ingest of the new part files
 into the Data Repo

Currently, each of these steps (the Argo Workflow, the Dataflow job, and
hitting the Jade API) is kicked off manually. The plan is to use [Airflow](https://github.com/apache/airflow)
to schedule a weekly job that runs all of these steps sequentially so that the flow
of new data into the Data Repo is automated.
