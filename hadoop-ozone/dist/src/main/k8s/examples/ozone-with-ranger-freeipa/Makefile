SHELL := /bin/bash
CLUSTER_NAME ?= ozone-ranger-ldap
NS ?= ozone-test

OZONE_REPO        ?= /work/sber/component-ozone
RANGER_PLUGIN_REPO?= /work/sber/component-ranger-plugins
OZONE_IMAGE       ?= apache/ozone-test:local
PROFILE_DIR       := $(shell pwd)

.PHONY: all image plugin ozone minikube up apply test logs down ldap-check status

all: up test

image: plugin ozone load-image

plugin:
	@echo "==> building ranger-ozone-plugin tarball (ipa-userstore included)"
	cd $(RANGER_PLUGIN_REPO) && mvn -pl :ranger-distro,:ranger-ozone-plugin,:ranger-ozone-plugin-shim,:ranger-ipa-userstore,:ranger-plugin-classloader,:ranger-plugins-common,:ranger-plugins-audit,:ranger-plugins-cred -am -P sdp-build-ranger-ozone-plugin package -DskipTests -q

ozone:
	@echo "==> packaging local Ozone dist"
	cd $(OZONE_REPO) && mvn -pl :ozone -am package -DskipTests -Dmaven.javadoc.skip=true -Drat.skip=true -q
	@echo "==> staging ranger plugin tarball into ozone repo for docker build"
	cp $(RANGER_PLUGIN_REPO)/target/ranger-2.4.0-ozone-plugin.tar.gz $(OZONE_REPO)/ranger-2.4.0-ozone-plugin.tar.gz
	cp $(PROFILE_DIR)/Dockerfile.ozone $(OZONE_REPO)/Dockerfile.ozone
	@echo "==> building $(OZONE_IMAGE)"
	docker build -t $(OZONE_IMAGE) -f $(OZONE_REPO)/Dockerfile.ozone $(OZONE_REPO)
	rm -f $(OZONE_REPO)/Dockerfile.ozone $(OZONE_REPO)/ranger-2.4.0-ozone-plugin.tar.gz

load-image:
	minikube --profile $(CLUSTER_NAME) image load $(OZONE_IMAGE)

minikube:
	minikube --profile $(CLUSTER_NAME) status >/dev/null 2>&1 || \
	  minikube --profile $(CLUSTER_NAME) start --memory 6144 --cpus 4 --kubernetes-version v1.30.0
	minikube --profile $(CLUSTER_NAME) image pull apache/ranger:2.4.0
	minikube --profile $(CLUSTER_NAME) image pull postgres:12
	minikube --profile $(CLUSTER_NAME) image pull bitnami/openldap:2.6
	minikube --profile $(CLUSTER_NAME) image pull curlimages/curl:8.10.1
	minikube --profile $(CLUSTER_NAME) image pull bitnami/kubectl:1.30
	minikube --profile $(CLUSTER_NAME) image pull busybox:1.36

up: minikube image apply

apply:
	./deploy.sh

test:
	@echo "==> waiting for prerequisite jobs"
	kubectl -n $(NS) wait --for=condition=complete job/ranger-policies-bootstrap --timeout=15m
	kubectl -n $(NS) wait --for=condition=complete job/acl-bootstrap             --timeout=10m
	@echo "==> waiting for acl-test (OM RPC)"
	-kubectl -n $(NS) wait --for=condition=complete --for=condition=failed job/acl-test --timeout=10m
	kubectl -n $(NS) logs job/acl-test
	@kubectl -n $(NS) get job/acl-test -o jsonpath='{.status.succeeded}' | grep -q '^1$$' \
	  || (echo "acl-test FAILED"; exit 1)
	@echo "==> waiting for acl-test-s3 (s3g)"
	-kubectl -n $(NS) wait --for=condition=complete --for=condition=failed job/acl-test-s3 --timeout=10m
	kubectl -n $(NS) logs job/acl-test-s3
	@kubectl -n $(NS) get job/acl-test-s3 -o jsonpath='{.status.succeeded}' | grep -q '^1$$' \
	  || (echo "acl-test-s3 FAILED"; exit 1)
	@echo "acl-test + acl-test-s3 PASSED"

ldap-check:
	@echo "==> verifying LDAP seed"
	kubectl -n $(NS) run ldap-probe --rm -it --restart=Never \
	  --image=bitnami/openldap:2.6 -- \
	  bash -c "ldapsearch -x -H ldap://ldap.$(NS).svc.cluster.local -D 'cn=admin,dc=example,dc=test' -w AdminPass1! -b 'dc=example,dc=test' '(|(objectClass=person)(objectClass=groupOfNames))' cn member uid"

status:
	kubectl -n $(NS) get pods,svc,jobs

logs:
	@for j in ranger-policies-bootstrap acl-bootstrap acl-test; do \
	  echo "==== $$j ====" ; \
	  kubectl -n $(NS) logs job/$$j --tail=200 2>/dev/null || true ; \
	done

down:
	kubectl delete -k . --ignore-not-found
	minikube --profile $(CLUSTER_NAME) delete || true
