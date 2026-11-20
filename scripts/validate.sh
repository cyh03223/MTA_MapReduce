#!/usr/bin/env bash
# Detect malformed rows (wrong field count, empty/NaN, wrong type, out of range). See mta.validate.Schema.
#   scripts/validate.sh <input csv|dir> <valid output dir>     (rejects go to <valid output dir>_rejects)
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/_java.sh   # picks a Java 17-25
IN="${1:?input}"; OUT="${2:?output}"
[[ -f target/mta-mapreduce-1.0.jar ]] || mvn -q -DskipTests package
java -Xmx2g -cp target/mta-mapreduce-1.0.jar mta.validate.ValidateDriver \
  -D mapreduce.framework.name=local -D fs.defaultFS=file:/// \
  "$IN" "$OUT" 2>&1 | { grep -v -E 'log4j|^(INFO|WARN)|^[0-9]{4}-' || true; }   # hide Hadoop log noise; keep java's exit status
