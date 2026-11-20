#!/usr/bin/env bash
# Keep only selected columns (map-only MapReduce job, Hadoop local mode).
#   scripts/project.sh <input csv|dir> <output dir> [col1,col2,...]
# Default columns: transit_timestamp,station_complex_id,station_complex,borough,ridership,transfers,latitude,longitude
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/_java.sh   # picks a Java 17-25
IN="${1:?input}"; OUT="${2:?output}"
[[ -f target/mta-mapreduce-1.0.jar ]] || mvn -q -DskipTests package
java -Xmx2g -cp target/mta-mapreduce-1.0.jar mta.project.ProjectDriver \
  -D mapreduce.framework.name=local -D fs.defaultFS=file:/// \
  "$IN" "$OUT" ${3:+"$3"} 2>&1 | { grep -v -E 'log4j|^(INFO|WARN)|^[0-9]{4}-' || true; }   # hide Hadoop log noise; keep java's exit status
