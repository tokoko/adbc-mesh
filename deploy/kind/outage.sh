#!/usr/bin/env bash
# What holds while Kyverno is down. Scaling to zero makes Kyverno delete its own webhooks, so
# failurePolicy doesn't apply: only what the API server enforces natively is left.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
export KUBECONFIG=$here/kubeconfig
NS=relay-test; DEPLOY=deploy/kyverno-admission-controller; failures=0
pass() { echo "PASS  $1"; }
fail() { echo "FAIL  $1"; failures=$((failures + 1)); }
restore() { kubectl -n kyverno scale $DEPLOY --replicas=1 >/dev/null; kubectl -n kyverno rollout status $DEPLOY --timeout=180s >/dev/null; }
trap restore EXIT
webhooks() { kubectl get validatingwebhookconfigurations,mutatingwebhookconfigurations -o name | grep -c kyverno-resource; }

labelled='{name: down-relay, namespace: '$NS', labels: {adbc-relay.io/backend: postgres}}'
check() { # name, expected message, metadata, spec
  local out; out=$(printf 'apiVersion: v1\nkind: Pod\nmetadata: %s\nspec:\n%s\n' "$3" "$4" | kubectl apply --dry-run=server -f - 2>&1)
  if [ $? -ne 0 ] && grep -qE "$2" <<<"$out"; then pass "$1"; else fail "$1: $(cut -c1-200 <<<"$out")"; fi
}
attacks() {
  check "credentials in an unlabelled pod" "only an injected relay pod|failed calling webhook" "{name: down-a, namespace: $NS}" \
    "  containers: [{name: app, image: relay-consumer:dev, volumeMounts: [{name: s, mountPath: /s}]}]
  volumes: [{name: s, secret: {secretName: adbc-relay-postgres}}]"
  check "relay pod that was never injected" "relay-sidecar must run first|failed calling webhook" "$labelled" \
    "  containers: [{name: app, image: relay-consumer:dev}]"
  check "pod carrying its own fake sidecar" "only an injected relay pod|failed calling webhook" "{name: down-c, namespace: $NS}" \
    "  initContainers: [{name: relay-sidecar, image: relay-sidecar:dev, command: [sh], volumeMounts: [{name: s, mountPath: /s}]}]
  containers: [{name: app, image: relay-consumer:dev}]
  volumes: [{name: s, secret: {secretName: adbc-relay-postgres}}]"
  check "minted vault-audience token" "vault audience|failed calling webhook" "{name: down-d, namespace: $NS}" \
    "  containers: [{name: app, image: relay-consumer:dev}]
  volumes: [{name: t, projected: {sources: [{serviceAccountToken: {path: token, audience: vault}}]}}]"
  check "privileged pod" "privileged|failed calling webhook" "{name: down-e, namespace: $NS}" \
    "  containers: [{name: app, image: relay-consumer:dev, securityContext: {privileged: true}}]"
  check "unapproved service account" "approved service account|failed calling webhook" "{name: down-f, namespace: $NS}" \
    "  serviceAccountName: some-powerful-account
  containers: [{name: app, image: relay-consumer:dev}]"
}

echo "-- Kyverno scaled to zero"
kubectl -n kyverno scale $DEPLOY --replicas=0 >/dev/null
kubectl -n kyverno wait --for=delete pod -l app.kubernetes.io/component=admission-controller --timeout=120s >/dev/null 2>&1
for _ in $(seq 30); do [ "$(webhooks)" = 0 ] && break; sleep 1; done
[ "$(webhooks)" = 0 ] && echo "      note: Kyverno removed its webhooks" || echo "      note: webhooks still present"
attacks
out=$(printf 'apiVersion: v1\nkind: Pod\nmetadata: {name: down-ok, namespace: relay-backend}\nspec:\n  containers: [{name: app, image: relay-consumer:dev}]\n' | kubectl apply --dry-run=server -f - 2>&1)
[ $? -eq 0 ] && pass "unrelated namespaces keep working" || fail "unrelated namespace blocked: $(cut -c1-200 <<<"$out")"
restore

echo "-- Kyverno crashed"
sleep 10
kubectl -n kyverno delete pod -l app.kubernetes.io/component=admission-controller --grace-period=0 --force >/dev/null 2>&1
attacks

for _ in $(seq 60); do [ "$(webhooks)" -gt 0 ] && break; sleep 1; done
sleep 15
echo "-- recovered"
out=$(printf 'apiVersion: v1\nkind: Pod\nmetadata: %s\nspec:\n  containers: [{name: app, image: relay-consumer:dev}]\n' "$labelled" | kubectl apply --dry-run=server -o jsonpath='{.spec.initContainers[*].name}' -f - 2>&1)
[ "$out" = relay-sidecar ] && pass "injection works again" || fail "injection after recovery: $(cut -c1-200 <<<"$out")"
echo; echo "$failures failure(s)"
[ "$failures" -eq 0 ]
