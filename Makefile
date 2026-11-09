REPO_URL      ?= https://github.com/omegazyadav/cf-assignment.git
GITHUB_TOKEN  ?= $(shell gh auth token)
ARGOCD_NS     ?= argocd
MODEL_NS      ?= mlops
FAMILY        ?= random-forest
VERSION       ?= 1-0-0
LOCAL_PORT    ?= 8000

.PHONY: repo-credentials install-ray install-models install uninstall status versions port-forward predict

repo-credentials:
	kubectl -n $(ARGOCD_NS) create secret generic cf-assignment-repo \
		--from-literal=type=git \
		--from-literal=url=$(REPO_URL) \
		--from-literal=username=omegazyadav \
		--from-literal=password=$(GITHUB_TOKEN) \
		--dry-run=client -o yaml | kubectl apply -f -
	kubectl -n $(ARGOCD_NS) label secret cf-assignment-repo argocd.argoproj.io/secret-type=repository --overwrite

install-ray: repo-credentials
	kubectl apply -f apps/kuberay-operator/application.yaml
	kubectl -n $(ARGOCD_NS) wait --for=jsonpath='{.status.health.status}'=Healthy application/kuberay-operator --timeout=300s

install-models:
	kubectl apply -f apps/models/applicationset.yaml

install: install-ray install-models

uninstall:
	kubectl -n $(ARGOCD_NS) delete applicationset mlmodel-ray --ignore-not-found
	kubectl -n $(ARGOCD_NS) delete application kuberay-operator --ignore-not-found

status:
	kubectl -n $(ARGOCD_NS) get applications
	kubectl -n $(MODEL_NS) get rayservices,pods

versions:
	@kubectl -n $(MODEL_NS) get rayservice -o json | python3 -c "import sys,json,re; [print(i['metadata']['name']+'\\n'+''.join('  '+m.group(1)+'\\n' for l in i['spec']['serveConfigV2'].splitlines() for m in [re.match(r'\\s*- name: (?!MLModelDeployment)(.+)',l)] if m)) for i in json.load(sys.stdin)['items']]"

predict:
	curl -s -X POST http://$(FAMILY)-$(VERSION).mlmodel-ray.localhost:8080/predict -H "Content-Type: application/json" -d '{"features":[5.1,3.5,1.4,0.2]}'
