REPO_URL      ?= https://github.com/omegazyadav/cf-assignment.git
GITHUB_TOKEN  ?= $(shell gh auth token)
ARGOCD_NS     ?= argocd
MODEL_NS      ?= mlops
FAMILY        ?= random-forest
VERSION       ?= 1-0-0
LOCAL_PORT    ?= 8000

.PHONY: repo-credentials install-ray install-models install uninstall status port-forward predict

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
	kubectl apply -f apps/random-forest/application.yaml
	kubectl apply -f apps/logistic-regression/application.yaml

install: install-ray install-models

uninstall:
	kubectl -n $(ARGOCD_NS) delete application mlmodel-ray-random-forest-1-0-0 mlmodel-ray-logistic-regression-1-0-0 kuberay-operator --ignore-not-found

status:
	kubectl -n $(ARGOCD_NS) get applications
	kubectl -n $(MODEL_NS) get rayservices,pods

port-forward:
	kubectl -n $(MODEL_NS) port-forward svc/mlmodel-ray-$(FAMILY)-$(VERSION)-serve-svc $(LOCAL_PORT):8000

predict:
	curl -s -X POST http://localhost:$(LOCAL_PORT)/$(FAMILY)-$(VERSION)/predict -H "Content-Type: application/json" -d '{"features":[5.1,3.5,1.4,0.2]}'
