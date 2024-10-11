DEV_BASE_IMAGE = us-central1-docker.pkg.dev/datalake-dev-375719/nutrisco-dev-data-service/data-service-base:0.0.2
PRD_BASE_IMAGE = us-central1-docker.pkg.dev/datalake-prd-375719/nutrisco-prd-data-service/data-service-base:0.0.1


update-branches:
	git remote prune origin
	python3 -m __scripts__.remove_old_git_branches

deploy-files-to-gcs:


download-bigquery-dependecies:
	gsutil cp gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar .

download-miniconda3-installer:
	wget https://repo.anaconda.com/miniconda/Miniconda3-py39_4.10.3-Linux-x86_64.sh

donwload-image-requirements: download-bigquery-dependecies download-miniconda3-


# build:
# 	docker build -t gcr.io/my-project/my-image:0.0.0 -f Dockerfile.dev .

deploy-base-image-dev:
	IMAGE=us-central1-docker.pkg.dev/datalake-dev-375719/nutrisco-dev-ingestion-service/test:0.0.2
	docker build -t "${IMAGE}" .
	docker push "${IMAGE}

deploy-base-image-prd:
	docker build -t "$(PRD_BASE_IMAGE)" -f images/base/Dockerfile .
	docker push "$(PRD_BASE_IMAGE)

zip file creation:
	python3 -m __cicd__.conversion_to_zip_file