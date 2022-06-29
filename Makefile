install:
	rm -rf Pipfile.lock
	pipenv install -d

deploy_staging:
	AWS_PROFILE=tw-vast-staging ENVIRONMENT=staging cdk deploy

deploy_production:
	AWS_PROFILE=tw-vast-production ENVIRONMENT=production cdk deploy

### commands for local testing

enqueue:
	pipenv run python -m src.enqueue_underlyings

process:
	pipenv run python -m src.process_underlying
