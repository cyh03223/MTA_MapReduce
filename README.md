# MTA Subway Hourly Ridership - MapReduce pipeline

Dataset: [MTA Subway Hourly Ridership 2020-2024](https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-2020-2024/wujg-7c2s) (Socrata `wujg-7c2s`).
Jobs run with Hadoop in local mode (`mvn` + Java only; no cluster needed).

## MapReduce code (`src/main/java/mta/`)
| File | Contents |
|---|---|
| `MTA_mapper.java` | all mappers (summary, frequency, project, aggregate, clean, validate, hours) + shared config keys, counters, helpers |
| `MTA_combiner.java` | combiners (summary, frequency, aggregate, hour count) |
| `MTA_reducer.java` | reducers (summary, top-K, aggregate, station quality, hour count) |
| `profile/ProfileDriver.java`, `project/`, `clean/`, `validate/`, `quality/` | one driver (job setup + report) per stage |
| `clean/CleanRules.java`, `validate/Schema.java` | the cleaning rules and the expected column schema |
| `profile/CsvLine.java`, `profile/ColumnStats.java` | CSV parsing/formatting and mergeable column statistics |

## Shell commands (`scripts/`)
| Step | Command | Output |
|---|---|---|
| Ingest | `scripts/ingest.sh all 2023 2024` | `data/mta_YYYY-MM.csv` (row counts verified against the API) |
| Project (keep 8 columns) | `scripts/project.sh data data_projected` | `data_projected/` |
| Aggregate (one row per station-hour) | `scripts/aggregate.sh data_projected data_hourly` | `data_hourly/` |
| Clean (borough short names, numeric station id) | `scripts/clean.sh data_hourly data_clean` | `data_clean/` |
| Validate (malformed rows) | `scripts/validate.sh data_clean data_valid` | `data_valid/`, `data_valid_rejects/` |
| Quality (duplicates, missing hours) | `scripts/quality.sh data_valid data_quality` | `data_quality/report.txt` |
| Profile one column | `scripts/profile.sh data_valid ridership 20` | `out/<column>/report.txt` |
| Everything except profiling | `scripts/pipeline.sh 2023 2024` | |

Known issue: 27 multi-entrance stations have several lat/lon pairs per `station_complex_id`, so the aggregate step leaves
duplicate (station, hour) rows for them (`quality.sh` reports them).
