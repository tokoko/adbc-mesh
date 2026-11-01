#!/usr/bin/env bash
# Create the kind cluster if needed and (re)deploy images, Kyverno, policies and the test backend.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd); root=$(cd "$here/../.." && pwd)
export KUBECONFIG=$here/kubeconfig
CLUSTER=adbc-relay
PG_DRIVER=${PG_DRIVER:-$HOME/.config/adbc/drivers/postgresql_linux_amd64_v1.12.0/libadbc_driver_postgresql.so}

kind get clusters | grep -qx $CLUSTER || kind create cluster --config "$here/cluster.yaml" --kubeconfig "$KUBECONFIG" --wait 120s

build=$(mktemp -d); trap 'rm -rf "$build"' EXIT
cp -r "$root/deploy/images/." "$build/"
cp "$root/target/release/relay-sidecar" "$PG_DRIVER" "$build/sidecar/"
cp "$root/target/release/libadbc_driver_relay.so" "$root/test/podsim/consumer_test.py" "$build/consumer/"
docker build -q -t relay-sidecar:dev "$build/sidecar" >/dev/null 2>&1
docker build -q -t relay-consumer:dev "$build/consumer" >/dev/null 2>&1
kind load docker-image --name $CLUSTER relay-sidecar:dev relay-consumer:dev >/dev/null

helm repo add kyverno https://kyverno.github.io/kyverno/ >/dev/null 2>&1 || true
helm upgrade --install kyverno kyverno/kyverno --version 3.9.2 -n kyverno --create-namespace --wait --timeout 5m >/dev/null
kubectl apply -f "$root/deploy/kyverno/inject-postgres.yaml" -f "$root/deploy/kyverno/validate.yaml"

kubectl apply -f "$here/backend.yaml"
kubectl -n relay-backend create configmap pg-seed --from-file=seed.sql="$root/test/podsim/seed.sql" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n relay-backend rollout status deployment/pg --timeout=300s
kubectl apply -f "$here/relay-test.yaml"
