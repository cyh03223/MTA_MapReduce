#!/usr/bin/env bash
# Apply the cleaning rules in mta.clean.CleanRules (map-only job).
#   scripts/clean.sh <input csv|dir> <output dir> [col1,col2,...]
set -euo pipefail
cd "$(dirname "$0")/.."
export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home 2>/dev/null || true)}"
IN="${1:?input}"; OUT="${2:?output}"
[[ -f target/mta-mapreduce-1.0.jar ]] || mvn -q -DskipTests package
java -Xmx2g -cp target/mta-mapreduce-1.0.jar mta.clean.CleanDriver \
  -D mapreduce.framework.name=local -D fs.defaultFS=file:/// \
  "$IN" "$OUT" ${3:+"$3"} 2>&1 | grep -v -E 'log4j|^(INFO|WARN)|^[0-9]{4}-'
