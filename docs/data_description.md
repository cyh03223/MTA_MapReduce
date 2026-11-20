# MTA Subway Hourly Ridership: Data Source and Processing

## 1. Data source

**Dataset:** MTA Subway Hourly Ridership: 2020–2024, published by the Metropolitan Transportation Authority on the New York State Open Data portal (data.ny.gov), Socrata dataset ID `wujg-7c2s`.
Link: https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-2020-2024/wujg-7c2s/about_data

**Content:** estimated subway ridership for each station complex, for each hour, split by payment method (e.g. MetroCard, OMNY) and fare class.

**Size:** 120,855,567 rows in total (metadata last updated 2025-10-15).

| Year | Rows |
|---|---|
| 2020 | 20,339,851 |
| 2021 | 23,281,980 |
| 2022 | 24,635,810 |
| 2023 | 25,585,413 |
| 2024 | 27,012,513 |

**Original columns (15):** `transit_timestamp`, `transit_mode`, `station_complex_id`, `station_complex`, `borough`, `payment_method`, `fare_class_category`, `ridership`, `transfers`, `latitude`, `longitude`, `georeference`, and three `:@computed_region_*` columns added by the portal.

**Subset used in this project:** 2023 and 2024 only: 52,597,926 rows (about 7.5 GB as CSV).

## 2. Ingestion

- The full-file bulk export was too slow (about 0.85 MB/s, an estimated 3–4 hours), and paging a whole year in one request failed with server timeouts (HTTP 408).
- Final method: the Socrata SODA API, **one request per day**, saved as one CSV per month (`data/mta_YYYY-MM.csv`, 24 files). Only the 11 non-computed columns were requested.
- **Verification:** after each month, the row count of the file was compared with the API's own count for that month. All 24 months match exactly (total 52,597,926 rows).
- Command: `scripts/ingest.sh all 2023 2024`

## 3. Processing (Hadoop MapReduce, local mode)

All steps are MapReduce jobs written in Java. Code: https://github.com/cyh03223/MTA_MapReduce (`MTA_mapper.java`, `MTA_combiner.java`, `MTA_reducer.java` plus one driver per stage; one shell script per step in `scripts/`).

### 3.1 Profiling
A profiling job computes, for any chosen column: row count, missing values, distinct values, min / max / mean / standard deviation (numeric columns), string length range, and the top-K most frequent values. The summary job merges per-mapper statistics; the frequency job is a word-count with a combiner and a top-K reducer.

Profile of `ridership` in the raw 2023–2024 data (52,597,926 rows): no missing values, 8,238 distinct values, min 1, max 16,217, mean 45.15, standard deviation 164.32. The distribution is strongly right-skewed (11% of rows are exactly 1). The minimum is 1, so hours with zero riders are not recorded.

### 3.2 Column projection (map-only job)
Kept 8 columns: `transit_timestamp`, `station_complex_id`, `station_complex`, `borough`, `ridership`, `transfers`, `latitude`, `longitude`.
Dropped: `transit_mode`, `payment_method`, `fare_class_category`, `georeference` and the computed-region columns. Row count unchanged (52,597,926).

### 3.3 Hourly aggregation (MapReduce with combiner)
Because payment method and fare class were dropped, many rows shared the same timestamp and station. The job groups rows by (timestamp, station id, station name, borough, latitude, longitude) and **sums `ridership` and `transfers`**.

- Result: 7,654,276 rows (one per station per hour).
- Checks: total ridership 2,374,859,865 and total transfers 98,680,068 are identical before and after (no data lost or double counted).
- Hourly `ridership`: mean 310.27, standard deviation 669.54, min 1, max 21,479; `transfers`: mean 12.89, max 2,561.

### 3.4 Cleaning rules (map-only job)
1. **`borough`** full names changed to short codes: Manhattan → MN, Queens → QN, Bronx → BX, Brooklyn → BK, Staten Island → SI. All 5 values were covered; no unmapped values.
2. **`station_complex_id`** converted to numeric. Only two ids were not numeric: `TRAM1` (RI Tramway, Manhattan) and `TRAM2` (RI Tramway, Roosevelt). Removing the letters would give 1 and 2, which already belong to Astoria-Ditmars Blvd and Astoria Blvd, so they were mapped to unused numbers **1001** and **1002** (31,559 rows). Result: 428 distinct ids, all numeric.

### 3.5 Validation (map-only job)
Each row is checked for: wrong number of fields, empty values, NaN / null / infinity tokens, wrong data type, and out-of-range values (timestamp must parse and fall in 2020–2024; ids and counts must be numbers; ridership and transfers ≥ 0; latitude/longitude inside the New York City area; borough one of the 5 codes). Bad rows would be written to a separate rejects file with the reason.

Result: **0 of 7,654,276 rows rejected.** The checks were also tested on hand-made bad rows, and each failure type was detected.

### 3.6 Duplicate and missing-hour detection (MapReduce)
- **Duplicates (same station and hour):** 247,954 duplicate keys in 27 stations (551 with identical values, 247,403 with conflicting values).
  **Cause:** these 27 multi-entrance stations report several slightly different latitude/longitude pairs under one `station_complex_id` (e.g. Court Sq has 6 pairs). The aggregation step in 3.3 included latitude and longitude in the grouping key, so these rows were not combined.
  **Effect:** ridership for those stations is split over several rows; the overall totals are still correct.
  **Not yet fixed.** Planned fix: group by timestamp and station id only, and use one coordinate per station (the average of its reported coordinates).
- **Missing hours:** 153,882 station-hours are missing across 372 stations (hours between a station's first and last row that have no row). The 856 hours skipped by daylight-saving "spring forward" were not counted. No hour is missing for all stations. Because the minimum ridership is 1, many missing hours are probably hours with zero riders rather than lost data; the longest gaps (up to about 300 hours, e.g. stations 282 and 286 in July 2024) look like station closures. This has not been confirmed with an outside source.

## 4. Data snippet

A small example from the 2023 and 2024 data: the raw rows for one station (Bowery, id 103) at 08:00 on two days, and the final rows after the pipeline. Latitude/longitude are kept in the raw rows; `transit_mode`, `payment_method` and `fare_class_category` are dropped in the final data.

**Raw, 2023 (2023-01-01 08:00, Bowery): 7 rows**

```
transit_timestamp,transit_mode,station_complex_id,station_complex,borough,payment_method,fare_class_category,ridership,transfers,latitude,longitude
2023-01-01T08:00:00.000,subway,103,"Bowery (J,Z)",Manhattan,metrocard,Metrocard - Full Fare,7.0,0.0,40.72028,-73.99391
2023-01-01T08:00:00.000,subway,103,"Bowery (J,Z)",Manhattan,metrocard,Metrocard - Other,1.0,0.0,40.72028,-73.99391
2023-01-01T08:00:00.000,subway,103,"Bowery (J,Z)",Manhattan,metrocard,Metrocard - Fair Fare,1.0,0.0,40.72028,-73.99391
2023-01-01T08:00:00.000,subway,103,"Bowery (J,Z)",Manhattan,metrocard,Metrocard - Seniors & Disability,1.0,0.0,40.72028,-73.99391
2023-01-01T08:00:00.000,subway,103,"Bowery (J,Z)",Manhattan,metrocard,Metrocard - Unlimited 30-Day,2.0,0.0,40.72028,-73.99391
2023-01-01T08:00:00.000,subway,103,"Bowery (J,Z)",Manhattan,metrocard,Metrocard - Unlimited 7-Day,10.0,0.0,40.72028,-73.99391
2023-01-01T08:00:00.000,subway,103,"Bowery (J,Z)",Manhattan,omny,OMNY - Full Fare,7.0,0.0,40.72028,-73.99391
```

**Raw, 2024 (2024-12-31 08:00, Bowery): 8 rows**

```
2024-12-31T08:00:00.000,subway,103,"Bowery (J,Z)",Manhattan,metrocard,Metrocard - Full Fare,2.0,0.0,40.72028,-73.99391
2024-12-31T08:00:00.000,subway,103,"Bowery (J,Z)",Manhattan,metrocard,Metrocard - Other,6.0,0.0,40.72028,-73.99391
2024-12-31T08:00:00.000,subway,103,"Bowery (J,Z)",Manhattan,metrocard,Metrocard - Unlimited 7-Day,12.0,0.0,40.72028,-73.99391
2024-12-31T08:00:00.000,subway,103,"Bowery (J,Z)",Manhattan,metrocard,Metrocard - Seniors & Disability,2.0,0.0,40.72028,-73.99391
2024-12-31T08:00:00.000,subway,103,"Bowery (J,Z)",Manhattan,metrocard,Metrocard - Fair Fare,5.0,0.0,40.72028,-73.99391
2024-12-31T08:00:00.000,subway,103,"Bowery (J,Z)",Manhattan,metrocard,Metrocard - Unlimited 30-Day,3.0,0.0,40.72028,-73.99391
2024-12-31T08:00:00.000,subway,103,"Bowery (J,Z)",Manhattan,omny,OMNY - Full Fare,41.0,0.0,40.72028,-73.99391
2024-12-31T08:00:00.000,subway,103,"Bowery (J,Z)",Manhattan,omny,OMNY - Students,1.0,0.0,40.72028,-73.99391
```

**Final (after projection, aggregation and cleaning): 5 rows**

```
transit_timestamp,station_complex_id,station_complex,borough,ridership,transfers,latitude,longitude
2023-01-01T08:00:00.000,1,"Astoria-Ditmars Blvd (N,W)",QN,119,2,40.775036,-73.91203
2023-01-01T08:00:00.000,103,"Bowery (J,Z)",MN,29,0,40.72028,-73.99391
2024-12-31T08:00:00.000,1,"Astoria-Ditmars Blvd (N,W)",QN,726,69,40.775036,-73.91203
2024-12-31T08:00:00.000,103,"Bowery (J,Z)",MN,72,0,40.72028,-73.99391
2023-06-07T14:00:00.000,1001,RI Tramway (Manhattan),MN,153,21,40.761337,-73.96416
```

- The 7 raw Bowery rows of 2023 sum to ridership 29 (7+1+1+1+2+10+7); the 8 raw rows of 2024 sum to 72 (2+6+12+2+5+3+41+1). Each becomes one final row, with `borough` shortened to `MN`.
- The last row shows the id cleaning: the raw id `TRAM1` (7 raw rows at that hour, ridership 153, transfers 21 in total) became `1001`.
- The Astoria-Ditmars rows are shown in final form only (many raw rows are combined into one).

## 5. Reproducing the results

```
scripts/pipeline.sh 2023 2024          # ingest, project, aggregate, clean, validate, quality
scripts/profile.sh data_valid ridership 20   # profile one column
```
