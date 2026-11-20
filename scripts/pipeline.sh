#!/usr/bin/env bash
# Full pipeline: ingest -> project -> aggregate -> clean -> validate -> quality.
#   scripts/pipeline.sh [YEAR...]       (default years: 2023 2024)
# Profiling is run separately per column:  scripts/profile.sh <dir> <column> [topK]
set -euo pipefail
cd "$(dirname "$0")/.."
scripts/ingest.sh all "$@"
scripts/project.sh   data           data_projected
scripts/aggregate.sh data_projected data_hourly
scripts/clean.sh     data_hourly    data_clean
scripts/validate.sh  data_clean     data_valid      # rejects -> data_valid_rejects
scripts/quality.sh   data_valid     data_quality    # report  -> data_quality/report.txt
