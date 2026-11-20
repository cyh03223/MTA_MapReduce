#!/usr/bin/env bash
# Run the whole pipeline on a tiny subset of the 2023 + 2024 data (39 raw rows, runs in about a minute):
#   - 2023-01-01 08:00  stations 1 (Astoria-Ditmars) and 103 (Bowery)
#   - 2023-06-07 14:00  station TRAM1 (Roosevelt Island Tramway; shows the id cleaning)
#   - 2024-12-31 08:00  stations 1 and 103
# Steps: extract subset -> project -> aggregate -> clean -> validate -> profile, then compare with the expected
# final rows (same rows as the data snippet in docs/data_description.md).
#
#   scripts/demo_subset.sh
#
# Rows come from data/mta_YYYY-MM.csv if you have ingested them, otherwise from the Socrata API (needs internet).
# Everything is written to data_subset/ (safe to delete); the main data folders and out/ are not touched.
set -euo pipefail
cd "$(dirname "$0")/.."

SUB=data_subset
rm -rf "$SUB"; mkdir -p "$SUB/raw"

# month-file | timestamp | station ids (comma separated)
SELECTION=(
  "2023-01|2023-01-01T08:00:00.000|1,103"
  "2023-06|2023-06-07T14:00:00.000|TRAM1"
  "2024-12|2024-12-31T08:00:00.000|1,103"
)

extract() { # $1=month $2=timestamp $3=ids  -> $SUB/raw/subset_$1.csv
  local out="$SUB/raw/subset_$1.csv" src="data/mta_$1.csv"
  if [[ -f "$src" ]]; then
    head -n 1 "$src" > "$out"
    awk -F, -v ts="$2" -v ids="$3" 'BEGIN{n=split(ids,a,","); for(i=1;i<=n;i++) want[a[i]]=1}
      {t=$1; id=$3; gsub(/"/,"",t); gsub(/"/,"",id)}   # the API quotes every field
      t==ts && (id in want)' "$src" >> "$out"
  else
    local list; list="'$(echo "$3" | sed "s/,/','/g")'"
    curl -fsSL -G "https://data.ny.gov/resource/wujg-7c2s.csv" \
      --data-urlencode "\$select=transit_timestamp,transit_mode,station_complex_id,station_complex,borough,payment_method,fare_class_category,ridership,transfers,latitude,longitude" \
      --data-urlencode "\$where=transit_timestamp='${2%.000}' AND station_complex_id in($list)" \
      --data-urlencode "\$order=:id" --data-urlencode "\$limit=1000" > "$out"
  fi
}

echo "== 1. extract subset"
for s in "${SELECTION[@]}"; do IFS='|' read -r m ts ids <<< "$s"; extract "$m" "$ts" "$ids"; done
echo "raw rows: $(cat "$SUB"/raw/*.csv | grep -vc '^"\{0,1\}transit_timestamp')"

echo "== 2. project (keep 8 columns)";    scripts/project.sh   "$SUB/raw"       "$SUB/projected"
echo "== 3. aggregate (station-hour)";    scripts/aggregate.sh "$SUB/projected" "$SUB/hourly"    | tail -n 5
echo "== 4. clean (borough, station id)"; scripts/clean.sh     "$SUB/hourly"    "$SUB/clean"     | tail -n 5
echo "== 5. validate";                    scripts/validate.sh  "$SUB/clean"     "$SUB/valid"     | tail -n 3
echo "== 6. profile ridership";           OUT_ROOT="$SUB/profile" scripts/profile.sh "$SUB/valid" ridership 3 | sed -n 2,9p

echo "== final rows"
grep -vh '^transit_timestamp' "$SUB"/valid/part-* | sort > "$SUB/final.csv"
head -n 1 "$SUB"/valid/part-m-00000
cat "$SUB/final.csv"

cat > "$SUB/expected.csv" <<'CSV'
2023-01-01T08:00:00.000,1,"Astoria-Ditmars Blvd (N,W)",QN,119,2,40.775036,-73.91203
2023-01-01T08:00:00.000,103,"Bowery (J,Z)",MN,29,0,40.72028,-73.99391
2023-06-07T14:00:00.000,1001,RI Tramway (Manhattan),MN,153,21,40.761337,-73.96416
2024-12-31T08:00:00.000,1,"Astoria-Ditmars Blvd (N,W)",QN,726,69,40.775036,-73.91203
2024-12-31T08:00:00.000,103,"Bowery (J,Z)",MN,72,0,40.72028,-73.99391
CSV
if diff -q <(sort "$SUB/expected.csv") "$SUB/final.csv" > /dev/null; then
  echo "PASS: final rows match the expected subset result"
else
  echo "FAIL: final rows differ from expected"; diff <(sort "$SUB/expected.csv") "$SUB/final.csv" || true; exit 1
fi
