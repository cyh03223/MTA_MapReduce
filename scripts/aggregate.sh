#!/usr/bin/env bash
# One row per station per hour: sums ridership and transfers over payment method / fare class.
#   scripts/aggregate.sh <projected csv|dir> <output dir>
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/_java.sh   # picks a Java 17-25
IN="${1:?input}"; OUT="${2:?output}"
[[ -f target/mta-mapreduce-1.0.jar ]] || mvn -q -DskipTests package
java -Xmx4g -cp target/mta-mapreduce-1.0.jar mta.project.AggregateDriver \
  -D mapreduce.framework.name=local -D fs.defaultFS=file:/// \
  "$IN" "$OUT" 2>&1 | { grep -v -E 'log4j|^(INFO|WARN)|^[0-9]{4}-' || true; }   # hide Hadoop log noise; keep java's exit status
