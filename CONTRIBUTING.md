## Google Cloud

Storage notifications cannot be created with the Console. [It requires `gsutil`](https://cloud.google.com/storage/docs/reporting-changes#enabling)

for example:

```shell
gsutil notification create -e OBJECT_FINALIZE -p 'input/' -t datalake-landing-events -f json gs://eqlab-datalake-landing
```

## Unit testing

for GCP, get credentials for **equancyrandd**

for AWS, get credentials for **equancy-lab**

```shell
export AWS_PROFILE='equancy-lab'
export GOOGLE_APPLICATION_CREDENTIALS='path/to/key.json'

poetry install
poetry run coverage run -m pytest && poetry run coverage report -m
```