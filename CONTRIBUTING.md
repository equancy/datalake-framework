## Google Cloud

Storage notifications cannot be created with the Console. [It requires `gsutil`](https://cloud.google.com/storage/docs/reporting-changes#enabling)

for example:

```shell
gsutil notification create -e OBJECT_FINALIZE -p 'input/' -t datalake-landing-events -f json gs://eqlab-datalake-landing
```

## Unit testing

launch a **development** datalake-catalog instance on port 8080

```shell
docker run --rm -d \
    --name catalog \
    -p 8080:8080 \
    -e CATALOG_WORKERS=1 \
    641143039263.dkr.ecr.eu-west-3.amazonaws.com/equancy-datalake/catalog:1.0.0-alpha.1
```

for GCP, get credentials for **equancyrandd**

for AWS, get a service account for **equancy-lab**

for Azure, get a service principal for **Azure Equancy**

```shell
export AWS_PROFILE='equancy-lab'
export GOOGLE_APPLICATION_CREDENTIALS='path/to/key.json'
export AZURE_TENANT_ID='4a134cf6-5468-45e4-859b-bd3cc08223a2'
export AZURE_CLIENT_ID='********-****-****-****-************'
export AZURE_CLIENT_SECRET='*************************************'

poetry install
poetry run coverage run -m pytest && poetry run coverage report -m
```