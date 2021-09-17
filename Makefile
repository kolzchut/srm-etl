.DEFAULT_GOAL := help
SHELL := /bin/bash

####

.PHONY: help
help:
	@echo "Use \`make <target>' where <target> is one of"
	@grep -E '^\.PHONY: [a-zA-Z_-]+ .*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = "(: |##)"}; {printf "\033[36m%-30s\033[0m %s\n", $$2, $$3}'

####

.PHONY: to_dp ## Create a data package with all usable data from staging.
to_dp:
	python -m operators.derive.to_dp

.PHONY: to_mapbox ## Create a derived geojson database for Mapbox from data staging.
to_mapbox:
	python -m operators.derive.to_mapbox

.PHONY: to_sql ## Create a derived SQL database from data staging.
to_sql:
	python -m operators.derive.to_sql

.PHONY: to_es ## Create a derived search index from data staging.
to_es:
	python -m operators.derive.to_es

.PHONY: derive ## Create all derived databases.
derive:
	python -m operators.derive.__init__

####
