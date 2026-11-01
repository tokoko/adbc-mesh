#!/usr/bin/env bash
# Admission tests through the real API server, then the attack suite as an injected Job.
set -uo pipefail
# The last element of a pipeline runs in this shell, so refused() can count failures.
shopt -s lastpipe
here=$(cd "$(dirname "$0")" && pwd)
export KUBECONFIG=$here/kubeconfig
NS=relay-test; failures=0
pass() { echo "PASS  $1"; }
fail() { echo "FAIL  $1"; failures=$((failures + 1)); }

# usage: refused <name> <expected message> <<< pod-yaml
refused() {
  local out; out=$(kubectl apply --dry-run=server -f - 2>&1)
  if [ $? -ne 0 ] && grep -qE "$2" <<<"$out"; then pass "$1"; else fail "$1: $(cut -c1-220 <<<"$out")"; fi
}
# usage: allowed <name> <<< pod-yaml. Guards against policies that block too much.
allowed() {
  local out; out=$(kubectl apply --dry-run=server -f - 2>&1)
  if [ $? -eq 0 ]; then pass "$1"; else fail "$1: $(cut -c1-220 <<<"$out")"; fi
}
pod() { # name, labels-yaml, spec-yaml
  printf 'apiVersion: v1\nkind: Pod\nmetadata:\n  name: %s\n  namespace: %s\n  labels: %s\nspec:\n%s\n' "$1" "$NS" "$2" "$3"
}
L='{adbc-relay.io/backend: postgres}'
APP='  containers:
    - {name: app, image: relay-consumer:dev'
SEC='  volumes:
    - {name: s, secret: {secretName: adbc-relay-postgres}}'

echo "-- admission: credentials"
pod t-unlabelled '{}' "$APP, volumeMounts: [{name: s, mountPath: /s}]}
$SEC" | refused "unlabelled pod mounting the secret" "only an injected relay pod"
pod t-mount "$L" "$APP, volumeMounts: [{name: s, mountPath: /s}]}
$SEC" | refused "consumer mounting the secret" "only relay-sidecar may mount"
pod t-mount-injected "$L" "$APP, volumeMounts: [{name: relay-secret, mountPath: /s}]}" | refused "consumer mounting the injected volume" "only relay-sidecar may mount"
pod t-projected "$L" "$APP, volumeMounts: [{name: p, mountPath: /s}]}
  volumes:
    - {name: p, projected: {sources: [{secret: {name: adbc-relay-postgres}}]}}" | refused "secret through a projected volume" "only relay-sidecar may mount"
pod t-env '{}' "$APP, env: [{name: X, valueFrom: {secretKeyRef: {name: adbc-relay-postgres, key: uri}}}]}" | refused "secret through env" "environment variables"
pod t-envfrom '{}' "$APP, envFrom: [{secretRef: {name: adbc-relay-postgres}}]}" | refused "secret through envFrom" "environment variables"

echo "-- admission: impersonating or dropping the sidecar"
pod t-dup-sidecar "$L" "$APP}
    - {name: relay-sidecar, image: evil}" | refused "user container named relay-sidecar" "Duplicate value"
pod t-dup-init "$L" "  initContainers:
    - {name: relay-sidecar, image: evil}
$APP}" | refused "user init container named relay-sidecar" "Duplicate value"
pod t-nonsense '{adbc-relay.io/backend: nonsense}' "$APP}" | refused "label with no injection policy" "relay-sidecar must run first"

echo "-- admission: escaping the sandbox"
pod t-root "$L" "$APP, securityContext: {runAsUser: 0}}" | refused "consumer as root" "non-root"
pod t-netadmin "$L" "$APP, securityContext: {capabilities: {add: [NET_ADMIN]}}}" | refused "consumer with NET_ADMIN" "add capabilities|non-default capabilities"
pod t-hostnet "$L" "  hostNetwork: true
$APP}" | refused "hostNetwork" "host namespaces"
pod t-sharepid "$L" "  shareProcessNamespace: true
$APP}" | refused "shared process namespace" "process namespace"
pod t-hostpath "$L" "$APP}
  volumes:
    - {name: h, hostPath: {path: /}}" | refused "hostPath" "host paths|hostPath volumes"
pod t-token "$L" "$APP}
  volumes:
    - {name: t, projected: {sources: [{serviceAccountToken: {path: t}}]}}" | refused "hand-rolled service account token" "service account tokens"
pod t-token-mount "$L" "$APP, volumeMounts: [{name: relay-token, mountPath: /t}]}" | refused "consumer mounting the sidecar's token" "only relay-sidecar may mount the service account token"
pod t-token-dup "$L" "$APP}
  volumes:
    - {name: relay-token, projected: {sources: [{serviceAccountToken: {path: token, audience: evil}}]}}" | refused "user volume named relay-token" "Duplicate value"

echo "-- admission: pods with no relay label"
pod u-host '{}' "$APP, securityContext: {privileged: true}, volumeMounts: [{name: h, mountPath: /host}]}
  volumes:
    - {name: h, hostPath: {path: /}}" | refused "privileged with hostPath" "privileged|host paths|hostPath"
pod u-priv '{}' "$APP, securityContext: {privileged: true}}" | refused "privileged" "privileged"
pod u-path '{}' "$APP}
  volumes:
    - {name: h, hostPath: {path: /}}" | refused "hostPath" "host paths|hostPath"
pod u-net '{}' "  hostNetwork: true
$APP}" | refused "hostNetwork" "host namespaces"
pod u-pid '{}' "  hostPID: true
$APP}" | refused "hostPID" "host namespaces"
pod u-cap '{}' "$APP, securityContext: {capabilities: {add: [SYS_ADMIN]}}}" | refused "SYS_ADMIN" "add capabilities|non-default capabilities"
pod u-vault '{}' "$APP, volumeMounts: [{name: t, mountPath: /t}]}
  volumes:
    - {name: t, projected: {sources: [{serviceAccountToken: {path: token, audience: vault}}]}}" | refused "minting a vault-audience token" "vault audience"
pod u-vault2 '{}' "$APP, volumeMounts: [{name: relay-token, mountPath: /t}]}
  volumes:
    - {name: relay-token, projected: {sources: [{serviceAccountToken: {path: token, audience: vault}}]}}" | refused "vault-audience token in a volume named relay-token" "vault audience"
pod u-sa '{}' "  serviceAccountName: some-powerful-account
$APP}" | refused "unapproved service account" "approved service account"
pod t-sa "$L" "  serviceAccountName: some-powerful-account
$APP}" | refused "unapproved service account on a relay pod" "approved service account"

echo "-- admission: Pod Security stays on"
printf 'apiVersion: v1\nkind: Namespace\nmetadata:\n  name: t-no-pss\n  labels: {adbc-relay.io/enabled: "true"}\n' | refused "enabling a namespace without Pod Security" "must enforce the baseline"
printf 'apiVersion: v1\nkind: Namespace\nmetadata:\n  name: t-weak-pss\n  labels: {adbc-relay.io/enabled: "true", pod-security.kubernetes.io/enforce: privileged}\n' | refused "enabling a namespace with privileged Pod Security" "must enforce the baseline"
out=$(kubectl label namespace $NS pod-security.kubernetes.io/enforce- --dry-run=server 2>&1)
[ $? -ne 0 ] && grep -qF "must enforce the baseline" <<<"$out" && pass "removing Pod Security from an enabled namespace" || fail "removing Pod Security: $(cut -c1-200 <<<"$out")"

echo "-- admission: normal use stays allowed"
pod ok-plain '{}' "$APP}" | allowed "plain unlabelled pod"
pod ok-relay "$L" "$APP}" | allowed "plain relay pod"
pod ok-init "$L" "  initContainers:
    - {name: setup, image: relay-consumer:dev}
$APP}" | allowed "relay pod with its own init container"
pod ok-secret "$L" "$APP, env: [{name: X, valueFrom: {secretKeyRef: {name: my-app-secret, key: k}}}], volumeMounts: [{name: m, mountPath: /m}]}
  volumes:
    - {name: m, secret: {secretName: my-app-secret}}" | allowed "relay pod using its own secret"
pod ok-token '{}' "$APP, volumeMounts: [{name: t, mountPath: /t}]}
  volumes:
    - {name: t, projected: {sources: [{serviceAccountToken: {path: token, audience: my-service}}]}}" | allowed "unlabelled pod with a token for another audience"

echo "-- running pods"
kubectl -n $NS delete job consumer --ignore-not-found --wait=true >/dev/null
kubectl apply -f "$here/consumer-job.yaml" >/dev/null
pod_name=
for _ in $(seq 30); do
  pod_name=$(kubectl -n $NS get pods -l job-name=consumer -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) && [ -n "$pod_name" ] && break
  sleep 1
done
if [ -z "$pod_name" ]; then
  fail "job created no pod"; kubectl -n $NS describe job consumer | tail -5 | cut -c1-300
  echo "$failures failure(s)"; exit 1
fi

# Things only a live pod can answer.
for _ in $(seq 60); do
  [ "$(kubectl -n $NS get pod "$pod_name" -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null)" = true ] && break
  [ "$(kubectl -n $NS get pod "$pod_name" -o jsonpath='{.status.phase}' 2>/dev/null)" != Pending ] && break
  sleep 1
done
order=$(kubectl -n $NS get pod "$pod_name" -o jsonpath='{.spec.initContainers[*].name}')
[ "$order" = "relay-sidecar" ] && pass "injected in order: $order" || fail "init order: $order"

patch=$(printf '{"spec":{"initContainers":[{"name":"relay-sidecar","image":"evil"}]}}')
out=$(kubectl -n $NS patch pod "$pod_name" --dry-run=server -p "$patch" 2>&1)
[ $? -ne 0 ] && grep -qF "approved image" <<<"$out" && pass "swapping the sidecar image on a live pod" || fail "image swap: $(cut -c1-200 <<<"$out")"
out=$(kubectl -n $NS patch pod "$pod_name" --dry-run=server --type=json -p '[{"op":"remove","path":"/metadata/labels/adbc-relay.io~1backend"}]' 2>&1)
[ $? -ne 0 ] && grep -qE "only an injected relay pod|keep the adbc-relay.io/backend label" <<<"$out" && pass "removing the label on a live pod" || fail "label removal: $(cut -c1-200 <<<"$out")"
# The default debug profile adds SYS_PTRACE, so Pod Security answers first. The baseline profile adds
# nothing, so it reaches our own rule.
out=$(kubectl -n $NS debug "$pod_name" --image=busybox --target=app -- true 2>&1)
[ $? -ne 0 ] && grep -qE "ephemeral containers are not allowed|SYS_PTRACE" <<<"$out" && pass "kubectl debug --target on a live pod" || fail "ephemeral with target: $(tail -1 <<<"$out" | cut -c1-200)"
out=$(kubectl -n $NS debug "$pod_name" --image=busybox --profile=baseline -- true 2>&1)
[ $? -ne 0 ] && grep -qF "ephemeral containers are not allowed" <<<"$out" && pass "kubectl debug --profile=baseline on a live pod" || fail "ephemeral: $(tail -1 <<<"$out" | cut -c1-200)"
[ -z "$(kubectl -n $NS get pod "$pod_name" -o jsonpath='{.spec.ephemeralContainers[*].name}')" ] && pass "no ephemeral container was added" || fail "an ephemeral container got in"

echo; echo "-- sidecar-only token"
kubectl -n $NS delete pod token-probe --ignore-not-found --wait=true >/dev/null
pod token-probe "$L" "  terminationGracePeriodSeconds: 1
$APP, command: [sleep, '600']}" | kubectl apply -f - >/dev/null
kubectl -n $NS wait --for=condition=Ready pod/token-probe --timeout=120s >/dev/null
TOKEN_PATH=/var/run/secrets/adbc-relay/token
if jwt=$(kubectl -n $NS exec token-probe -c relay-sidecar -- cat $TOKEN_PATH 2>/dev/null) && [ -n "$jwt" ]; then pass "sidecar can read the token"; else fail "sidecar cannot read the token: $(kubectl -n $NS exec token-probe -c relay-sidecar -- ls -lL $TOKEN_PATH 2>&1 | cut -c1-120)"; fi
kubectl -n $NS exec token-probe -c app -- sh -c "! test -e $TOKEN_PATH && ! test -e /var/run/secrets/kubernetes.io" && pass "consumer has no token at all" || fail "consumer can see a token"
kubectl -n $NS exec token-probe -c relay-sidecar -- sh -c "! test -e /var/run/secrets/kubernetes.io" && pass "sidecar has no default API token" || fail "sidecar has the default API token"
review() { printf '{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","spec":{"token":"%s","audiences":["%s"]}}' "$jwt" "$1" | kubectl create -f - -o jsonpath='{.status.authenticated}/{.status.user.username}' 2>/dev/null; }
r=$(review vault); [ "$r" = "true/system:serviceaccount:$NS:default" ] && pass "token verifies for audience vault as $r" || fail "TokenReview for vault: $r"
r=$(review https://kubernetes.default.svc.cluster.local); [ "${r%%/*}" != true ] && pass "token is rejected for the API server audience" || fail "token accepted by API audience: $r"
kubectl -n $NS delete pod token-probe --wait=false >/dev/null

echo; echo "-- attack suite inside the injected pod"
if kubectl -n $NS wait --for=condition=complete job/consumer --timeout=300s >/dev/null 2>&1; then job_ok=1; else job_ok=0; fi
kubectl -n $NS logs "$pod_name" -c app 2>&1 | cut -c1-150
[ $job_ok = 1 ] && pass "consumer job completed, and the native sidecar let it finish" || fail "consumer job did not complete"
# The pod only finishes once the sidecar handles SIGTERM. Otherwise this takes the full grace period.
code=
for _ in $(seq 15); do
  code=$(kubectl -n $NS get pod "$pod_name" -o jsonpath='{.status.initContainerStatuses[?(@.name=="relay-sidecar")].state.terminated.exitCode}' 2>/dev/null)
  [ -n "$code" ] && break
  sleep 1
done
[ "$code" = 0 ] && pass "sidecar exits cleanly and promptly on SIGTERM" || fail "sidecar shutdown: exit code '$code' within 15s"
echo; echo "-- sidecar"
kubectl -n $NS get pod "$pod_name" -o jsonpath='{range .status.initContainerStatuses[*]}{.name}: restarts={.restartCount} state={.state}{"\n"}{end}' | cut -c1-160
kubectl -n $NS logs "$pod_name" -c relay-sidecar 2>&1 | grep -vE '^connection from|^$|^Query was' | cut -c1-140 | head -24
echo; echo "$failures failure(s) outside the attack suite"
[ "$failures" -eq 0 ] && [ $job_ok = 1 ]
