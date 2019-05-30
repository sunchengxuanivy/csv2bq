# the env var is required.
# should run this command without set-env-vars first. then run it again with set-env-vars,
# since the service name will not be known until it's created
gcloud beta run deploy ivy-gcr --image="gcr.io/endpoints-release/endpoints-runtime-serverless:1.30.0" \
    --allow-unauthenticated \
    --project=mark-l-240702 --set-env-vars ENDPOINTS_SERVICE_NAME=ivy-gcr-7jgvnh5pma-uc.a.run.app

gcloud endpoints services deploy openapi-run.yaml  --project mark-l-240702

