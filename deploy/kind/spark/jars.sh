#!/usr/bin/env bash
# Build the spark-adbc connector from the last commit of a checkout, ignoring uncommitted changes, and
# collect it with the ADBC jars the Spark image doesn't already have. Uses that checkout's pixi env.
set -euo pipefail
root=$(cd "$(dirname "$0")/../../.." && pwd)
SPARK_ADBC=${SPARK_ADBC:-$HOME/spark-adbc}
out=$root/target/spark-jars
src=$(mktemp -d); trap 'rm -rf "$src"' EXIT
git -C "$SPARK_ADBC" archive HEAD | tar -x -C "$src"
export PATH=$SPARK_ADBC/.pixi/envs/default/bin:$PATH JAVA_HOME=$SPARK_ADBC/.pixi/envs/default/lib/jvm
classpath=$(cd "$src" && sbt -batch -error root/package 'export root/Test/fullClasspath' | tail -1)
rm -rf "$out"; mkdir -p "$out"
cp "$src"/target/scala-2.13/spark-adbc_*.jar "$out/"
tr ':' '\n' <<<"$classpath" | grep -E '/(adbc-core|adbc-driver-jni|adbc-driver-manager|arrow-c-data)-[0-9.]+\.jar$' | xargs -I{} cp {} "$out/"
ls "$out"
