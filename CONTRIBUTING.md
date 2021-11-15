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
    641143039263.dkr.ecr.eu-west-3.amazonaws.com/equancy-datalake/catalog:1.0.0-alpha.0
```

for GCP, get credentials for **equancyrandd**

for AWS, get credentials for **equancy-lab**

```shell
export AWS_PROFILE='equancy-lab'
export GOOGLE_APPLICATION_CREDENTIALS='path/to/key.json'

poetry install
poetry run coverage run -m pytest && poetry run coverage report --omit='tests/*' -m
```