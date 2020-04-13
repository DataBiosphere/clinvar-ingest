# ClinVar Ingest
Batch ETL pipeline to mirror ClinVar releases into the Jade Data Repository.

### How this ingest process works

Ingest orchestration is driven by [Argo](https://github.com/argoproj/argo). A stand-alone
WorkflowTemplate defines the steps:
1. Generate a temporary local volume
2. Download the latest ClinVar release from the [ClinVar FTP server](ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/clinvar_variation/)
   to the volume
3. In parallel:
   1. Upload the raw release to GCS using [gsutil](https://github.com/GoogleCloudPlatform/gsutil)
   2. Extract the release into JSON-list:
      1. Generate another local volume to store extracted data
      2. Run the compressed XML release through our [XML -> JSON tool](https://github.com/broadinstitute/monster-xml-to-json-list),
         storing the outputs on the 2nd volume
      3. Upload the JSON-list files to GCS

In the near future, we will add additional steps to the workflow:
1. [Dataflow](https://cloud.google.com/dataflow/) to transform the raw extracted JSON into the desired output schema
2. BigQuery jobs to diff new data against the rows already in dev
3. Jade API calls to ingest the raw archive and transformed JSON rows
