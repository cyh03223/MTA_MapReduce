#!/usr/bin/env bash
# Build and run the profiler in Hadoop local mode.
#   scripts/run.sh <input csv|dir> <column> [topK]
set -euo pipefail
cd "$(dirname "$0")/.."
export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home 2>/dev/null || true)}"
IN="${1:?input}"; COL="${2:?column}"; K="${3:-20}"
OUT_ROOT="${OUT_ROOT:-out}"   # where reports go (default: out/<column>)
[[ -f target/mta-mapreduce-1.0.jar ]] || mvn -q -DskipTests package
rm -rf "$OUT_ROOT/$COL"
java -Xmx4g -jar target/mta-mapreduce-1.0.jar \
  -D mapreduce.framework.name=local -D fs.defaultFS=file:/// \
  "$IN" "$OUT_ROOT/$COL" "$COL" "$K" 2>&1 | { grep -v -E 'log4j|^(INFO|WARN)|^[0-9]{4}-' || true; }
