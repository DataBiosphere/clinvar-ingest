# Argo + ClinVar
Note that this is EXPERIMENTAL for now.

The workflow runs successfully in GKE until the upload step, where it gets
a permission denied error. The plan is to use Workload Identities to make
the k8s SA running the workflow a Storage Admin on the output bucket.
