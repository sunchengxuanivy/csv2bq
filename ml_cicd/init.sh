gcloud iam service-accounts create ml-dataproc --project mark-l-240702

gcloud projects add-iam-policy-binding mark-l-240702 --member='serviceAccount:ml-dataproc@mark-l-240702.iam.gserviceaccount.com' --role='roles/bigquery.dataEditor'
gcloud projects add-iam-policy-binding mark-l-240702 --member='serviceAccount:ml-dataproc@mark-l-240702.iam.gserviceaccount.com' --role='roles/bigquery.jobUser'
gcloud projects add-iam-policy-binding mark-l-240702 --member='serviceAccount:ml-dataproc@mark-l-240702.iam.gserviceaccount.com' --role='roles/dataproc.worker'
gcloud projects add-iam-policy-binding mark-l-240702 --member='serviceAccount:ml-dataproc@mark-l-240702.iam.gserviceaccount.com' --role='roles/storage.objectAdmin'

gsutil mb -p mark-l-240702 gs://ml-dataproc-bucket-mark-l-240702
gsutil mb -p mark-l-240702 gs://main-scripts-bucket-mark-l-240702