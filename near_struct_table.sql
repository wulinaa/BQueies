-- The raw location data has been ingested into `sojern-warehouse.near.daily_foot_traffic`, where it collects the location of every device at any time.
  
WITH u AS (
  SELECT DISTINCT
    hashed_device_id,
    unix_timestamp_of_visit,
    visit_datetime_utc,
    visit_date_utc,
    --ROUND(SAFE_CAST(Lat_of_Visit AS FLOAT64), 2) as lat_of_visit,
    --ROUND(SAFE_CAST(Lon_of_Visit AS FLOAT64), 2) as lon_of_visit,
    CONCAT(
    IFNULL(SAFE_CAST(ROUND(SAFE_CAST(Lat_of_Visit AS FLOAT64), 2) AS STRING),""), "_",
    IFNULL(SAFE_CAST(ROUND(SAFE_CAST(Lon_of_Visit AS FLOAT64), 2) AS STRING),""), "_",
    IFNULL(country_code,""), "_",IFNULL(SAFE_CAST(census AS STRING),""), "_", IFNULL(Micro,""), "_", IFNULL(municipality,""), "_", IFNULL(Admin,""), "_", IFNULL(Province,""), "_", IFNULL(Postal,"")) as geo, lat_of_visit,lon_of_visit,country_code,country_code_iso3,census,micro,municipality,admin,province,postal,visit_time_local,visit_dayofweek_local,timezone,poi_id
  FROM `sojern-warehouse.near.daily_foot_traffic`
  WHERE _PARTITIONDATE = "2023-08-01"
  -- between "2023-08-01" and "2023-08-31"
  and hashed_device_id = "ea35d1f59b83697b611edd64ba5e5517c353f34a" --"076cffbee0bae77b3395d655d3b923a3bda045cf"
)

, l AS (
  SELECT
    *,
    LEAD(geo) OVER (PARTITION BY hashed_device_id, visit_date_utc ORDER BY visit_datetime_utc) AS lead_geo,
    LEAD(u.visit_datetime_utc) OVER (PARTITION BY hashed_device_id, visit_date_utc ORDER BY visit_datetime_utc) AS lead_time,
    ROW_NUMBER() OVER (PARTITION BY hashed_device_id, visit_date_utc, geo ORDER BY visit_datetime_utc) AS number_geo,
    ROW_NUMBER() OVER (PARTITION BY hashed_device_id, visit_date_utc, geo ORDER BY visit_datetime_utc DESC) AS number_geo_desc,
  FROM u
)
, min_max AS (
  SELECT *
  FROM l
  WHERE number_geo = 1 OR number_geo_desc = 1
)
, maxts AS (
  SELECT
  * EXCEPT (visit_datetime_utc),
  -- hashed_device_id,
  -- visit_date_utc,
  visit_datetime_utc AS min_ts,
  LEAD(visit_datetime_utc) OVER (PARTITION BY hashed_device_id, visit_date_utc, geo ORDER BY visit_datetime_utc) AS max_ts,
  -- geo
  FROM min_max
)

SELECT 
  * EXCEPT (max_ts),
  CASE WHEN number_geo = number_geo_desc THEN min_ts ELSE max_ts END AS max_ts
  FROM maxts
WHERE number_geo = 1
;

SELECT
  hashed_device_id,
  ARRAY_AGG(STRUCT( lat_of_visit,
  lon_of_visit,
  country_code,
  country_code_iso3,
  census,
  micro,
  municipality,
  admin,
  province,
  postal,
  geo )) AS geographic,
  ARRAY_AGG(STRUCT(
  min_ts,
  max_ts,
  unix_timestamp_of_visit,
  visit_date_utc,
  --visit_datetime_local,
  --visit_date_local,
  visit_time_local,
  visit_dayofweek_local,
  timezone )) AS time_cols,
FROM (
SELECT * from `sojern-warehouse-dev.temp.new_daily_foot_traffic_all_cols_1`)
GROUP BY 1
