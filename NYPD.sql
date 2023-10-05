# CLEAN NULL VALUES

SELECT * FROM bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions
WHERE location IS NOT NULL AND location != '(0.0, 0.0)'
AND (off_street_name IS NOT NULL OR (on_street_name IS NOT NULL AND cross_street_name IS NOT NULL)) 
AND (vehicle_type_code1 IS NOT NULL OR vehicle_type_code2 IS NOT NULL OR vehicle_type_code_3 IS NOT NULL OR vehicle_type_code_4 IS NOT NULL OR vehicle_type_code_5 IS NOT NULL) 
AND (contributing_factor_vehicle_1 IS NOT NULL OR contributing_factor_vehicle_2 IS NOT NULL OR contributing_factor_vehicle_3 IS NOT NULL OR contributing_factor_vehicle_4 IS NOT NULL OR contributing_factor_vehicle_5 IS NOT NULL) 

   
# CLEAN AND UPDATE BOROUGH DATA

WITH true_bor AS (
SELECT clean.unique_key, boundary.BoroName
FROM midyear-glazing-366016.nypd_version2.clean_nulls AS clean, midyear-glazing-366016.nypd_version2.borough_boundaries AS boundary
WHERE ST_WITHIN(ST_GEOGPOINT(clean.longitude, clean.latitude), ST_GEOGFROMTEXT(boundary.the_geom)))


SELECT clean_nulls.*, true_bor.BoroName AS true_borough
FROM midyear-glazing-366016.nypd_version2.clean_nulls AS clean_nulls
JOIN true_bor
ON clean_nulls.unique_key = true_bor.unique_key;

# CLEAN AND UPDATE STREET/INTERSECTION DATA

WITH new_st AS (
SELECT unique_key, on_street_name, off_street_name,
  CASE
    WHEN on_street_name IS NOT NULL THEN on_street_name
    WHEN off_street_name IS NOT NULL THEN off_street_name
  END AS street_name
FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors)

SELECT clean_nulls_bor.*, new_st.street_name AS street_name
FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors AS clean_nulls_bor
JOIN new_st
ON clean_nulls_bor.unique_key = new_st.unique_key;

-- ALTER TABLE midyear-glazing-366016.nypd_version2.clean_nulls_bors_onstreet
-- DROP COLUMN on_street_name,
-- DROP COLUMN off_street_name;

# CLEAN VEHICLE DATA
   
   WITH clean_vehicle AS (SELECT *,
CASE 
  WHEN LOWER(vehicle_type_code1) LIKE '%sedan%' THEN "Car"
  WHEN LOWER(vehicle_type_code1) LIKE '%3-door%' THEN "Car"
  WHEN LOWER(vehicle_type_code1) LIKE '%vehicle%' THEN "Car"
  WHEN LOWER(vehicle_type_code1) LIKE '%e-scooter%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code1) LIKE '%e-bike%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code1) LIKE '%pk%' THEN "Other"
  WHEN LOWER(vehicle_type_code1) LIKE '%tanker%' THEN "Truck"
  WHEN LOWER(vehicle_type_code1) LIKE '%suv%' THEN "Car"
  WHEN LOWER(vehicle_type_code1) LIKE '%large com%' THEN "Truck"
  WHEN LOWER(vehicle_type_code1) LIKE '%small com%' THEN "Car"
  WHEN LOWER(vehicle_type_code1) LIKE '%unknown%' THEN "Other"
  WHEN LOWER(vehicle_type_code1) LIKE '%wagon%' THEN "Car"
  WHEN LOWER(vehicle_type_code1) LIKE '%limo%' THEN "Car"
  WHEN LOWER(vehicle_type_code1) LIKE '%bike%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code1) LIKE '%e bik%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code1) LIKE '%flat%' THEN "Truck"
  WHEN LOWER(vehicle_type_code1) LIKE '%truck%' THEN "Truck"
  WHEN LOWER(vehicle_type_code1) LIKE '%amb%' THEN "Official/Emergency"
  WHEN LOWER(vehicle_type_code1) LIKE '%scoot%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code1) LIKE '%bus%' THEN "Bus"
  WHEN LOWER(vehicle_type_code1) LIKE '%taxi%' THEN "Taxi"
  WHEN LOWER(vehicle_type_code1) LIKE '%van%' THEN "Van"
  WHEN LOWER(vehicle_type_code1) LIKE '%cab%' THEN "Taxi"
  WHEN LOWER(vehicle_type_code1) LIKE '%motor%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code1) LIKE '%dump%' THEN "Truck"
  WHEN LOWER(vehicle_type_code1) LIKE '%camper%' THEN "Van"
  WHEN LOWER(vehicle_type_code1) LIKE '%carry%' THEN "Truck"
  WHEN LOWER(vehicle_type_code1) LIKE '%cycle%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code1) LIKE '%covert%' THEN "Car"
  WHEN LOWER(vehicle_type_code1) LIKE '%garb%' THEN "Truck"
  WHEN LOWER(vehicle_type_code1) LIKE '%mop%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code1) is null THEN null
  ELSE "Other"
  END as new_vehicle1,
CASE 
  WHEN LOWER(vehicle_type_code2) LIKE '%sedan%' THEN "Car"
  WHEN LOWER(vehicle_type_code2) LIKE '%3-door%' THEN "Car"
  WHEN LOWER(vehicle_type_code2) LIKE '%vehicle%' THEN "Car"
  WHEN LOWER(vehicle_type_code2) LIKE '%e-scooter%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code2) LIKE '%e-bike%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code2) LIKE '%pk%' THEN "Other"
  WHEN LOWER(vehicle_type_code2) LIKE '%tanker%' THEN "Truck"
  WHEN LOWER(vehicle_type_code2) LIKE '%suv%' THEN "Car"
  WHEN LOWER(vehicle_type_code2) LIKE '%large com%' THEN "Truck"
  WHEN LOWER(vehicle_type_code2) LIKE '%small com%' THEN "Car"
  WHEN LOWER(vehicle_type_code2) LIKE '%unknown%' THEN "Other"
  WHEN LOWER(vehicle_type_code2) LIKE '%wagon%' THEN "Car"
  WHEN LOWER(vehicle_type_code2) LIKE '%limo%' THEN "Car"
  WHEN LOWER(vehicle_type_code2) LIKE '%bike%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code2) LIKE '%e bik%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code2) LIKE '%flat%' THEN "Truck"
  WHEN LOWER(vehicle_type_code2) LIKE '%truck%' THEN "Truck"
  WHEN LOWER(vehicle_type_code2) LIKE '%amb%' THEN "Official/Emergency"
  WHEN LOWER(vehicle_type_code2) LIKE '%scoot%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code2) LIKE '%bus%' THEN "Bus"
  WHEN LOWER(vehicle_type_code2) LIKE '%taxi%' THEN "Taxi"
  WHEN LOWER(vehicle_type_code2) LIKE '%van%' THEN "Van"
  WHEN LOWER(vehicle_type_code2) LIKE '%cab%' THEN "Taxi"
  WHEN LOWER(vehicle_type_code2) LIKE '%motor%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code2) LIKE '%dump%' THEN "Truck"
  WHEN LOWER(vehicle_type_code2) LIKE '%camper%' THEN "Van"
  WHEN LOWER(vehicle_type_code2) LIKE '%carry%' THEN "Truck"
  WHEN LOWER(vehicle_type_code2) LIKE '%cycle%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code2) LIKE '%covert%' THEN "Car"
  WHEN LOWER(vehicle_type_code2) LIKE '%garb%' THEN "Truck"
  WHEN LOWER(vehicle_type_code2) LIKE '%mop%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code2) is null THEN null
  ELSE "Other"
  END as new_vehicle2,
CASE 
  WHEN LOWER(vehicle_type_code_3) LIKE '%sedan%' THEN "Car"
  WHEN LOWER(vehicle_type_code_3) LIKE '%3-door%' THEN "Car"
  WHEN LOWER(vehicle_type_code_3) LIKE '%vehicle%' THEN "Car"
  WHEN LOWER(vehicle_type_code_3) LIKE '%e-scooter%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_3) LIKE '%e-bike%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_3) LIKE '%pk%' THEN "Other"
  WHEN LOWER(vehicle_type_code_3) LIKE '%tanker%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_3) LIKE '%suv%' THEN "Car"
  WHEN LOWER(vehicle_type_code_3) LIKE '%large com%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_3) LIKE '%small com%' THEN "Car"
  WHEN LOWER(vehicle_type_code_3) LIKE '%unknown%' THEN "Other"
  WHEN LOWER(vehicle_type_code_3) LIKE '%wagon%' THEN "Car"
  WHEN LOWER(vehicle_type_code_3) LIKE '%limo%' THEN "Car"
  WHEN LOWER(vehicle_type_code_3) LIKE '%bike%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_3) LIKE '%e bik%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_3) LIKE '%flat%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_3) LIKE '%truck%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_3) LIKE '%amb%' THEN "Official/Emergency"
  WHEN LOWER(vehicle_type_code_3) LIKE '%scoot%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_3) LIKE '%bus%' THEN "Bus"
  WHEN LOWER(vehicle_type_code_3) LIKE '%taxi%' THEN "Taxi"
  WHEN LOWER(vehicle_type_code_3) LIKE '%van%' THEN "Van"
  WHEN LOWER(vehicle_type_code_3) LIKE '%cab%' THEN "Taxi"
  WHEN LOWER(vehicle_type_code_3) LIKE '%motor%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_3) LIKE '%dump%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_3) LIKE '%camper%' THEN "Van"
  WHEN LOWER(vehicle_type_code_3) LIKE '%carry%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_3) LIKE '%cycle%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_3) LIKE '%covert%' THEN "Car"
  WHEN LOWER(vehicle_type_code_3) LIKE '%garb%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_3) LIKE '%mop%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_3) is null THEN null
  ELSE "Other"
  END as new_vehicle3,
CASE 
  WHEN LOWER(vehicle_type_code_4) LIKE '%sedan%' THEN "Car"
  WHEN LOWER(vehicle_type_code_4) LIKE '%3-door%' THEN "Car"
  WHEN LOWER(vehicle_type_code_4) LIKE '%vehicle%' THEN "Car"
  WHEN LOWER(vehicle_type_code_4) LIKE '%e-scooter%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_4) LIKE '%e-bike%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_4) LIKE '%pk%' THEN "Other"
  WHEN LOWER(vehicle_type_code_4) LIKE '%tanker%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_4) LIKE '%suv%' THEN "Car"
  WHEN LOWER(vehicle_type_code_4) LIKE '%large com%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_4) LIKE '%small com%' THEN "Car"
  WHEN LOWER(vehicle_type_code_4) LIKE '%unknown%' THEN "Other"
  WHEN LOWER(vehicle_type_code_4) LIKE '%wagon%' THEN "Car"
  WHEN LOWER(vehicle_type_code_4) LIKE '%limo%' THEN "Car"
  WHEN LOWER(vehicle_type_code_4) LIKE '%bike%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_4) LIKE '%e bik%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_4) LIKE '%flat%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_4) LIKE '%truck%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_4) LIKE '%amb%' THEN "Official/Emergency"
  WHEN LOWER(vehicle_type_code_4) LIKE '%scoot%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_4) LIKE '%bus%' THEN "Bus"
  WHEN LOWER(vehicle_type_code_4) LIKE '%taxi%' THEN "Taxi"
  WHEN LOWER(vehicle_type_code_4) LIKE '%van%' THEN "Van"
  WHEN LOWER(vehicle_type_code_4) LIKE '%cab%' THEN "Taxi"
  WHEN LOWER(vehicle_type_code_4) LIKE '%motor%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_4) LIKE '%dump%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_4) LIKE '%camper%' THEN "Van"
  WHEN LOWER(vehicle_type_code_4) LIKE '%carry%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_4) LIKE '%cycle%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_4) LIKE '%covert%' THEN "Car"
  WHEN LOWER(vehicle_type_code_4) LIKE '%garb%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_4) LIKE '%mop%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_4) is null THEN null
  ELSE "Other"
  END as new_vehicle4,
CASE 
  WHEN LOWER(vehicle_type_code_5) LIKE '%sedan%' THEN "Car"
  WHEN LOWER(vehicle_type_code_5) LIKE '%3-door%' THEN "Car"
  WHEN LOWER(vehicle_type_code_5) LIKE '%vehicle%' THEN "Car"
  WHEN LOWER(vehicle_type_code_5) LIKE '%e-scooter%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_5) LIKE '%e-bike%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_5) LIKE '%pk%' THEN "Other"
  WHEN LOWER(vehicle_type_code_5) LIKE '%tanker%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_5) LIKE '%suv%' THEN "Car"
  WHEN LOWER(vehicle_type_code_5) LIKE '%large com%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_5) LIKE '%small com%' THEN "Car"
  WHEN LOWER(vehicle_type_code_5) LIKE '%unknown%' THEN "Other"
  WHEN LOWER(vehicle_type_code_5) LIKE '%wagon%' THEN "Car"
  WHEN LOWER(vehicle_type_code_5) LIKE '%limo%' THEN "Car"
  WHEN LOWER(vehicle_type_code_5) LIKE '%bike%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_5) LIKE '%e bik%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_5) LIKE '%flat%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_5) LIKE '%truck%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_5) LIKE '%amb%' THEN "Official/Emergency"
  WHEN LOWER(vehicle_type_code_5) LIKE '%scoot%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_5) LIKE '%bus%' THEN "Bus"
  WHEN LOWER(vehicle_type_code_5) LIKE '%taxi%' THEN "Taxi"
  WHEN LOWER(vehicle_type_code_5) LIKE '%van%' THEN "Van"
  WHEN LOWER(vehicle_type_code_5) LIKE '%cab%' THEN "Taxi"
  WHEN LOWER(vehicle_type_code_5) LIKE '%motor%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_5) LIKE '%dump%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_5) LIKE '%camper%' THEN "Van"
  WHEN LOWER(vehicle_type_code_5) LIKE '%carry%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_5) LIKE '%cycle%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_5) LIKE '%covert%' THEN "Car"
  WHEN LOWER(vehicle_type_code_5) LIKE '%garb%' THEN "Truck"
  WHEN LOWER(vehicle_type_code_5) LIKE '%mop%' THEN "Scooter/Bike"
  WHEN LOWER(vehicle_type_code_5) is null THEN null
  ELSE "Other"
  END as new_vehicle5
  
FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors_st)
SELECT * FROM clean_vehicle

WITH veh_list AS(
SELECT new_vehicle1 as veh1, count(*) as count1 FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors_st_veh
GROUP BY new_vehicle1
UNION ALL
SELECT new_vehicle2 as veh1, count(*) as count1 FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors_st_veh
GROUP BY new_vehicle2
UNION ALL
SELECT new_vehicle3 as veh1, count(*) as count1 FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors_st_veh
GROUP BY new_vehicle3
UNION ALL
SELECT new_vehicle5 as veh1, count(*) as count1 FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors_st_veh
GROUP BY new_vehicle5
UNION ALL
SELECT new_vehicle4 as veh1, count(*) as count1 FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors_st_veh
GROUP BY new_vehicle4)
SELECT veh_list.veh1,SUM(veh_list.count1) as sum from veh_list
group by veh_list.veh1
order by sum desc

SELECT * FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors_st_veh
WHERE new_vehicle1 IS NOT NULL


# COLLISIONS BY STREET/INTERSECTION


-- WITH intersection AS (
-- SELECT location, street_name, cross_street_name, COUNT(*) AS accidents
-- FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors_onstreet
-- GROUP BY location, street_name, cross_street_name)

-- SELECT MAX_BY((location, street_name, cross_street_name, accidents), accidents) AS fixed_street
-- FROM intersection
-- GROUP BY location;


WITH test_intersection AS (
SELECT clean_nulls_bors.*, fixed.fixed_street._field_2 AS on_street, fixed.fixed_street._field_3 AS cross_street
FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors AS clean_nulls_bors
JOIN midyear-glazing-366016.nypd_version2.fixed_street AS fixed
ON clean_nulls_bors.location = fixed.fixed_street._field_1)

-- ALTER TABLE midyear-glazing-366016.nypd_version2.clean_nulls_bors_st
-- DROP COLUMN on_street_name, 
-- DROP COLUMN off_street_name, 
-- DROP COLUMN cross_street_name;


-- SELECT *, CONCAT(latitude, ', ', longitude) AS loc
-- FROM `midyear-glazing-366016.nypd_version2.clean_nulls_bors_st` 


SELECT location, on_street, cross_street, COUNT(*) AS accidents
FROM test_intersection
GROUP BY location, on_street, cross_street
ORDER BY accidents DESC

# DATA FOR VEHICLES INVOLVED IN COLLISIONS

WITH vehicle AS (
SELECT new_vehicle1 as vehiclex, unique_key,  timestamp, location,true_borough FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors_st_veh
WHERE new_vehicle1 is not null

UNION ALL
SELECT new_vehicle2 as vehiclex, unique_key, timestamp, location,true_borough FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors_st_veh
WHERE new_vehicle2 is not null

UNION ALL
SELECT new_vehicle3 as vehiclex, unique_key,  timestamp, location,true_borough FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors_st_veh
WHERE new_vehicle3 is not null

UNION ALL
SELECT new_vehicle4 as vehiclex, unique_key,  timestamp, location,true_borough FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors_st_veh
WHERE new_vehicle4 is not null

UNION ALL
SELECT new_vehicle5 as vehiclex, unique_key, timestamp, location,true_borough FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors_st_veh
WHERE new_vehicle5 is not null
)

SELECT *
FROM vehicle

# DATA FOR FACTORS INVOLVED IN COLLISIONS


WITH factor AS (
SELECT contributing_factor_vehicle_1 as factorx, unique_key,  timestamp, location,true_borough FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors_st_veh
WHERE contributing_factor_vehicle_1 is not null

UNION ALL
SELECT contributing_factor_vehicle_2 as factorx, unique_key,  timestamp, location,true_borough FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors_st_veh
WHERE contributing_factor_vehicle_2 is not null

UNION ALL
SELECT contributing_factor_vehicle_3 as factorx, unique_key,  timestamp, location,true_borough FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors_st_veh
WHERE contributing_factor_vehicle_3 is not null

UNION ALL
SELECT contributing_factor_vehicle_4 as factorx, unique_key,  timestamp, location,true_borough FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors_st_veh
WHERE contributing_factor_vehicle_4 is not null

UNION ALL
SELECT contributing_factor_vehicle_5 as factorx, unique_key,  timestamp, location,true_borough FROM midyear-glazing-366016.nypd_version2.clean_nulls_bors_st_veh
WHERE contributing_factor_vehicle_5 is not null
)

SELECT *
FROM factor

  


# ARIMA FORECASTING

CREATE OR REPLACE MODEL nypd_version2.collision_forecast
OPTIONS
  (model_type = 'ARIMA_PLUS',
   time_series_timestamp_col = 'date',
   time_series_data_col = 'tot_collisions',
   horizon = 1095,
   auto_arima = TRUE,
   auto_arima_max_order = 5,
   DATA_FREQUENCY = 'DAILY',
   HOLIDAY_REGION = 'US',
   CLEAN_SPIKES_AND_DIPS = TRUE,
   DECOMPOSE_TIME_SERIES = TRUE
  ) AS
(SELECT
   EXTRACT(DATE from timestamp) AS date,
   count(*) AS tot_collisions
   
FROM `midyear-glazing-366016.nypd_version2.clean_vehicle_clean_borough`
group by date
);

SELECT
  *
FROM
  ML.EXPLAIN_FORECAST(MODEL  nypd_version2.collision_forecast,
                       STRUCT(1095 AS horizon, 0.9 AS confidence_level))



#K-MEANS CLUSTERING

CREATE OR REPLACE MODEL
  nypd_version2.test_clusters2 OPTIONS(model_type='kmeans',
    num_clusters = 5) AS(
SELECT
  contributing_factor_vehicle_1, contributing_factor_vehicle_2, new_vehicle1, new_vehicle2, Hour_of_Day, Day_of_week, Type_of_Street, intersection
FROM
  `midyear-glazing-366016.nypd_version2.clean_vehicle_clean_borough`
WHERE contributing_factor_vehicle_1 != 'Unspecified' AND contributing_factor_vehicle_2 != 'Unspecified'AND new_vehicle1 != 'Other Vehicular'AND new_vehicle2 != 'Other Vehicular');

SELECT
  * EXCEPT(nearest_centroids_distance)
FROM
  ML.PREDICT( MODEL nypd_version2.test_clusters2,
    (
    SELECT
      contributing_factor_vehicle_1, contributing_factor_vehicle_2, new_vehicle1, new_vehicle2, Hour_of_Day, Day_of_week, Type_of_Street, intersection
    FROM
      `midyear-glazing-366016.nypd_version2.clean_vehicle_clean_borough`
      WHERE contributing_factor_vehicle_1 != 'Unspecified' AND contributing_factor_vehicle_2 != 'Unspecified' AND new_vehicle1 != 'Other Vehicular'AND new_vehicle2 != 'Other Vehicular'))








