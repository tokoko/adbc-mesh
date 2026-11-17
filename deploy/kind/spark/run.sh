#!/usr/bin/env bash
# Build the Spark image, then spark-submit a job that reads Postgres through the relay and tries to
# abuse the driver's token. Needs deploy/kind/up.sh and deploy/kind/spark/jars.sh first.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd); root=$(cd "$here/../../.." && pwd)
export KUBECONFIG=$here/../kubeconfig
SPARK_ADBC_JARS=${SPARK_ADBC_JARS:-$root/target/spark-jars}
ls "$SPARK_ADBC_JARS"/spark-adbc_*.jar >/dev/null || { echo "run deploy/kind/spark/jars.sh first"; exit 1; }
NS=relay-spark; CLUSTER=adbc-mesh; IMAGE=relay-spark:dev
scratch=$(mktemp -d); trap 'rm -rf "$scratch"' EXIT

mkdir "$scratch/ctx" "$scratch/ctx/jars"
cp "$SPARK_ADBC_JARS"/*.jar "$scratch/ctx/jars/"
cp "$root/target/release/libadbc_driver_relay.so" "$root/test/spark_relay_test.py" "$here/../../images/spark/Dockerfile" "$scratch/ctx/"
cp -L "$root/.pixi/envs/default/lib/libstdc++.so.6" "$scratch/ctx/"
docker build -q -t $IMAGE "$scratch/ctx" >/dev/null 2>&1 || { echo "image build failed"; exit 1; }
# kind load can't import this image, because its multi-platform base is only partly present locally
# and Docker's containerd store exports the index anyway. Flattening leaves a plain single-platform image.
flatten() {
  local id changes=(); id=$(docker create "$1") || return 1
  while IFS= read -r env; do changes+=(--change "ENV $env"); done < <(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$1" | grep .)
  changes+=(--change "USER $(docker inspect -f '{{.Config.User}}' "$1")")
  changes+=(--change "WORKDIR $(docker inspect -f '{{.Config.WorkingDir}}' "$1")")
  changes+=(--change "ENTRYPOINT $(docker inspect -f '{{json .Config.Entrypoint}}' "$1")")
  docker export "$id" | docker import "${changes[@]}" - "$1" >/dev/null; local rc=$?
  docker rm "$id" >/dev/null; return $rc
}
flatten $IMAGE || { echo "flatten failed"; exit 1; }
kind load docker-image --name $CLUSTER $IMAGE >/dev/null || exit 1

kubectl apply -f "$here/tenant.yaml" >/dev/null || exit 1
kubectl -n $NS delete pod --all --wait=true >/dev/null 2>&1

# The submitter is the trusted platform, so it uses the admin kubeconfig. Only the driver's rights matter.
kind get kubeconfig --internal --name $CLUSTER > "$scratch/kubeconfig"
chmod a+r "$scratch/kubeconfig"; chmod a+rx "$scratch"; cp "$here/pod-template.yaml" "$scratch/pod-template.yaml"
docker run --rm --network kind -v "$scratch:/submit:ro" -e KUBECONFIG=/submit/kubeconfig -e HOME=/tmp $IMAGE \
  /opt/spark/bin/spark-submit \
  --master k8s://https://$CLUSTER-control-plane:6443 \
  --deploy-mode cluster --name relay-test \
  --conf spark.kubernetes.namespace=$NS \
  --conf spark.kubernetes.container.image=$IMAGE \
  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-driver \
  --conf spark.kubernetes.authenticate.executor.serviceAccountName=spark-executor \
  --conf spark.kubernetes.driver.label.adbc-mesh.io/backend=postgres \
  --conf spark.kubernetes.executor.label.adbc-mesh.io/backend=postgres \
  --conf spark.kubernetes.driver.podTemplateFile=/submit/pod-template.yaml \
  --conf spark.kubernetes.executor.podTemplateFile=/submit/pod-template.yaml \
  --conf spark.kubernetes.executor.deleteOnTermination=false \
  --conf spark.kubernetes.driver.ownPersistentVolumeClaim=false \
  --conf spark.kubernetes.driver.reusePersistentVolumeClaim=false \
  --conf spark.executor.instances=2 --conf spark.executor.memory=1g --conf spark.driver.memory=1g \
  --conf spark.kubernetes.submission.waitAppCompletion=true \
  local:///opt/relay/spark_relay_test.py > "$scratch/submit.log" 2>&1
submit=$?
echo "spark-submit exit=$submit"
driver=$(kubectl -n $NS get pods -l spark-role=driver -o name | head -1)
[ -n "$driver" ] || { grep -E 'Exception|Message:|denied|Forbidden' "$scratch/submit.log" | cut -c1-900 | head -12; exit 1; }
kubectl -n $NS logs "$driver" -c spark-kubernetes-driver > "$scratch/driver.log" 2>&1
grep -E '^(PASS|FAIL|-- |RESULT)' "$scratch/driver.log"
grep -E '^RESULT 0 failure' "$scratch/driver.log" >/dev/null; result=$?
if [ $result -ne 0 ]; then echo "-- driver log (errors)"; grep -E 'Exception|Error|Forbidden|forbidden' "$scratch/driver.log" | cut -c1-220 | head -20; fi

echo "-- pods"
kubectl -n $NS get pods -o custom-columns=NAME:.metadata.name,SA:.spec.serviceAccountName,PHASE:.status.phase,INIT:.spec.initContainers[*].name,AUTOMOUNT:.spec.automountServiceAccountToken
for pod in $(kubectl -n $NS get pods -l spark-role=executor -o name); do
  echo "-- $pod sidecar connections: $(kubectl -n $NS logs "$pod" -c relay-sidecar 2>/dev/null | grep -c '^connection from')"
done
# spark-submit exits 0 even when the driver fails, so the driver's own result is what counts.
sidecar_log=$(kubectl -n $NS logs "$driver" -c relay-sidecar 2>/dev/null)
if grep -qE 'permission denied for table employees|permission denied for column' <<<"$sidecar_log"; then
  echo "PASS  Postgres grants are what refused the hidden column"
else
  echo "FAIL  no Postgres permission error in the driver's sidecar log"; result=1
fi
[ $submit -eq 0 ] && [ $result -eq 0 ]
