
.DEFAULT_GOAL := help
.PHONY: help lint test grep
SHELL := bash


help: ## show target summary
	@grep -E '^\S+:.* ## .+$$' $(MAKEFILE_LIST) | sed 's/##/#/' | while IFS='#' read spec help; do \
	  tgt=$${spec%%:*}; \
	  printf "\n%s: %s\n" "$$tgt" "$$help"; \
	  awk -F ': ' -v TGT="$$tgt" '$$1 == TGT && $$2 ~ "=" { print $$2 }' $(MAKEFILE_LIST) | \
	  while IFS='#' read var help; do \
	    printf "  %s  :%s\n" "$$var" "$$help"; \
	  done \
	done


node_modules: package.json
	npm install
	touch node_modules


lint: node_modules ## run linters
	npm run lint


test: node_modules ## run unit tests
	npm run test


grep: TYPE=F# how to interpret NEEDLE (F=fixed, E=extended regex, G=basic regex)
grep: NEEDLE=todo# pattern to search for
grep: ## grep source
	grep -Hrni$(TYPE) -- '$(NEEDLE)' asset


asset.zip: LINUX=# build for linux target
asset.zip: asset/* ## build asset bundle
	if test -e asset.zip; then rm asset.zip; fi
	rsync -av --exclude node_modules asset/ build/
ifdef LINUX
	docker run -it --rm -e NODE_ENV=production -v $(PWD)/build/:/build/ -w /build node:8 yarn install --no-progress --pure-lockfile --link-duplicates
else
	cd build && yarn install --no-progress --pure-lockfile --link-duplicates
endif
	zip -x **/.DS_Store -vr asset.zip build > /dev/null

asset.install: TERASLICE=localhost:5678# cluster to deploy to
asset.install: asset.zip ## deploy asset bundle
	curl -sS -X POST -H 'Content-Type: application/octet-stream' $(TERASLICE)/assets --data-binary @asset.zip

asset.list: TERASLICE=localhost:5678# cluster to query
asset.list: ## list deployed assets
	curl -sS $(TERASLICE)/txt/assets/$(shell jq -r '.name' asset/asset.json)


job.install: TERASLICE=localhost:5678# cluster to deploy to
job.install: JOB=# job definition
job.install: ## deploy job
	curl -sS -X POST $(TERASLICE)/jobs --data @$(JOB)

job.list: TERASLICE=localhost:5678# cluster to query
job.list: ## list jobs
	curl -sS $(TERASLICE)/txt/jobs
