#!/usr/bin/env bash
# Detect duplicate (station, hour) rows and missing hours.
#   scripts/quality.sh <input csv|dir> <output dir>
set -euo pipefail
cd "$(dirname "$0")/.."
export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home 2>/dev/null || true)}"
IN="${1:?input}"; OUT="${2:?output}"
[[ -f target/mta-mapreduce-1.0.jar ]] || mvn -q -DskipTests package
java -Xmx4g -cp target/mta-mapreduce-1.0.jar mta.quality.QualityDriver \
  -D mapreduce.framework.name=local -D fs.defaultFS=file:/// \
  "$IN" "$OUT" 2>&1 | grep -v -E 'log4j|^(INFO|WARN)|^[0-9]{4}-'
