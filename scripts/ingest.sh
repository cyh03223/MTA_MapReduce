#!/usr/bin/env bash
# Export "MTA Subway Hourly Ridership: 2020-2024" (Socrata dataset wujg-7c2s, ~120.9M rows).
#
#   scripts/ingest.sh all [YEAR...]       # every month of the given years (default 2023 2024), 6 months in parallel
#   scripts/ingest.sh month YYYY MM       # one month, fetched day by day, row count verified -> data/mta_YYYY-MM.csv
#   scripts/ingest.sh year YYYY           # the 12 months of one year, sequentially
#   scripts/ingest.sh sample [N]          # first N rows (default 100000) for quick tests    -> data_sample/mta_sample.csv
#
# A month that is already verified (data/mta_YYYY-MM.csv.done) is skipped, so a failed run can simply be repeated.
# (Fetching a whole year with one paged request hits server timeouts; day-sized requests do not.)
# Optional: export SODA_APP_TOKEN=... to avoid API throttling.
set -euo pipefail

DS="wujg-7c2s"
HOST="https://data.ny.gov"
OUT_DIR="$(cd "$(dirname "$0")/.." && pwd)/data"
mkdir -p "$OUT_DIR"

# Columns kept for the API modes (drops georeference + :@computed_region_* noise).
COLS="transit_timestamp,transit_mode,station_complex_id,station_complex,borough,payment_method,fare_class_category,ridership,transfers,latitude,longitude"
TOKEN_HDR=()
[[ -n "${SODA_APP_TOKEN:-}" ]] && TOKEN_HDR=(-H "X-App-Token: $SODA_APP_TOKEN")

soda() { # $1 = urlencoded query string
  curl -fsSL --retry 10 --retry-delay 10 --retry-all-errors ${TOKEN_HDR[@]+"${TOKEN_HDR[@]}"} "$HOST/resource/$DS.csv?$1"
}

case "${1:-}" in
  sample)
    N="${2:-100000}"
    mkdir -p "$OUT_DIR/../data_sample"
    soda "\$select=$COLS&\$order=:id&\$limit=$N" > "$OUT_DIR/../data_sample/mta_sample.csv"
    ;;
  month)
    # One request per day (no deep offsets -> no server timeouts); verified against the API count.
    Y="${2:?year}"; M="$(printf %02d "$((10#${3:?month}))")"
    OUT="$OUT_DIR/mta_$Y-$M.csv"
    [[ -f "$OUT.done" ]] && { echo "$Y-$M already done" >&2; exit 0; }
    SKIP=0; [[ "${VERIFY_ONLY:-0}" == 1 && -s "$OUT" ]] && SKIP=1   # re-check an existing file without re-downloading
    [[ $SKIP == 0 ]] && : > "$OUT"
    for D in $(seq -w 1 31); do
      [[ $SKIP == 1 ]] && break
      date -j -f %Y-%m-%d "$Y-$M-$D" +%F >/dev/null 2>&1 || continue
      [[ "$(date -j -f %Y-%m-%d "$Y-$M-$D" +%m)" == "$M" ]] || continue
      WHERE="transit_timestamp%20between%20'$Y-$M-${D}T00:00:00'%20and%20'$Y-$M-${D}T23:59:59.999'"
      TMP="$(mktemp)"
      soda "\$select=$COLS&\$where=$WHERE&\$order=:id&\$limit=500000" > "$TMP"
      if [[ ! -s "$OUT" ]]; then cat "$TMP" >> "$OUT"; else tail -n +2 "$TMP" >> "$OUT"; fi
      rm -f "$TMP"
    done
    LAST=$(date -j -v+1m -v-1d -f %Y-%m-%d "$Y-$M-01" +%d)
    WHERE="transit_timestamp%20between%20'$Y-$M-01T00:00:00'%20and%20'$Y-$M-${LAST}T23:59:59.999'"
    EXPECT=$(soda "\$select=count(*)&\$where=$WHERE" | tail -n1 | tr -d '"')
    GOT=$(( $(wc -l < "$OUT") - 1 ))
    if [[ "$EXPECT" == "$GOT" ]]; then touch "$OUT.done"; echo "$Y-$M ok ($GOT rows)" >&2
    else echo "$Y-$M MISMATCH: expected $EXPECT got $GOT" >&2; exit 1; fi
    ;;
  year)
    Y="${2:?year required}"
    for M in $(seq 1 12); do "$0" month "$Y" "$M"; done
    ;;
  all)
    shift
    YEARS=("$@"); [[ ${#YEARS[@]} -eq 0 ]] && YEARS=(2023 2024)
    for Y in "${YEARS[@]}"; do for M in 01 02 03 04 05 06 07 08 09 10 11 12; do echo "$Y $M"; done; done |
      xargs -P "${PARALLEL:-6}" -L 1 sh -c 'exec "$0" month "$1" "$2"' "$0"
    ;;
  *)
    sed -n '2,10p' "$0"; exit 1;;
esac
echo "Done. Files in $OUT_DIR" >&2
