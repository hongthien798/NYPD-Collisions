#UPDATE ZIP CODE

WITH clean_zip AS(
SELECT 
   zip.zip_code AS new_zip, 
   nypd.unique_key,nypd.location,nypd.zip_code AS old_zip
FROM `midyear-glazing-366016.1234567.nypd_mn_collisions` as nypd
,`bigquery-public-data.geo_us_boundaries.zip_codes` as zip
WHERE ST_WITHIN(ST_GEOGPOINT(nypd.longitude, nypd.latitude),zip.zip_code_geom) AND nypd.zip_code IS null)

UPDATE `midyear-glazing-366016.1234567.nypd_mn_collisions` 
    SET zip_code2 = (
        SELECT new_zip
        FROM clean_zip
        WHERE nypd.unique_key = clean_zip.unique_key)
   
#UPDATE BOROUGH

WITH new_borough1 AS (

WITH new_table AS (
  SELECT *
  FROM `midyear-glazing-366016.nypd_version2.clean_zip` 
  WHERE location IS NOT NULL AND location != "(0.0, 0.0)" )

SELECT *, t2.borough as new_borough
FROM new_table nypd  

LEFT JOIN (
SELECT location as loc1, borough, max(counted)

FROM(
SELECT DISTINCT location , borough, COUNT (*) AS counted, COUNT (location) OVER (PARTITION BY location) AS counted_location
FROM new_table
GROUP BY location, borough
ORDER BY counted desc)

WHERE (counted_location = 1 and borough is not null) OR (counted_location >1 and borough is not null)

GROUP BY location, borough) t2
ON nypd.location = t2.loc1)

#UPDATE VEHICLE TYPES

WITH clean_vehicle AS (SELECT *,
CASE 
  WHEN LOWER(vehicle_type_code1) LIKE '%sedan%' THEN "car"
  WHEN LOWER(vehicle_type_code1) LIKE '%3-door%' THEN "car"
  WHEN LOWER(vehicle_type_code1) LIKE '%vehicle%' THEN "car"
  WHEN LOWER(vehicle_type_code1) LIKE '%e-scooter%' THEN "e-scooter"
  WHEN LOWER(vehicle_type_code1) LIKE '%e-bike%' THEN "e-scooter"
  WHEN LOWER(vehicle_type_code1) LIKE '%pk%' THEN "other"
  WHEN LOWER(vehicle_type_code1) LIKE '%tanker%' THEN "truck"
  WHEN LOWER(vehicle_type_code1) LIKE '%suv%' THEN "car"
  WHEN LOWER(vehicle_type_code1) LIKE '%large com%' THEN "truck"
  WHEN LOWER(vehicle_type_code1) LIKE '%small com%' THEN "car"
  WHEN LOWER(vehicle_type_code1) LIKE '%unknown%' THEN "other"
  WHEN LOWER(vehicle_type_code1) LIKE '%wagon%' THEN "car"
  WHEN LOWER(vehicle_type_code1) LIKE '%limo%' THEN "car"
  WHEN LOWER(vehicle_type_code1) LIKE '%bike%' THEN "bicycle"
  WHEN LOWER(vehicle_type_code1) LIKE '%e bik%' THEN "e-scooter"
  WHEN LOWER(vehicle_type_code1) LIKE '%flat%' THEN "truck"
  WHEN LOWER(vehicle_type_code1) LIKE '%truck%' THEN "truck"
  WHEN LOWER(vehicle_type_code1) LIKE '%amb%' THEN "ambulance"
  WHEN LOWER(vehicle_type_code1) LIKE '%scoot%' THEN "scooter"
  WHEN LOWER(vehicle_type_code1) LIKE '%bus%' THEN "bus"
  WHEN LOWER(vehicle_type_code1) LIKE '%taxi%' THEN "taxi"
  WHEN LOWER(vehicle_type_code1) LIKE '%van%' THEN "van"
  WHEN LOWER(vehicle_type_code1) LIKE '%cab%' THEN "taxi"
  WHEN LOWER(vehicle_type_code1) LIKE '%motor%' THEN "scooter"
  WHEN LOWER(vehicle_type_code1) LIKE '%dump%' THEN "truck"
  WHEN LOWER(vehicle_type_code1) LIKE '%camper%' THEN "van"
  WHEN LOWER(vehicle_type_code1) LIKE '%carry%' THEN "other"
  WHEN LOWER(vehicle_type_code1) LIKE '%cycle%' THEN "bicycle"
  WHEN LOWER(vehicle_type_code1) LIKE '%covert%' THEN "car"
  WHEN LOWER(vehicle_type_code1) LIKE '%garb%' THEN "truck"
  WHEN LOWER(vehicle_type_code1) LIKE '%mop%' THEN "scooter"
  WHEN LOWER(vehicle_type_code1) IS NULL THEN NULL
  ELSE "other"
  END as new_vehicle1, 
CASE 
  WHEN LOWER(vehicle_type_code2) LIKE '%sedan%' THEN "car"
  WHEN LOWER(vehicle_type_code2) LIKE '%3-door%' THEN "car"
  WHEN LOWER(vehicle_type_code2) LIKE '%vehicle%' THEN "car"
  WHEN LOWER(vehicle_type_code2) LIKE '%e-scooter%' THEN "e-scooter"
  WHEN LOWER(vehicle_type_code2) LIKE '%e-bike%' THEN "e-scooter"
  WHEN LOWER(vehicle_type_code2) LIKE '%pk%' THEN "other"
  WHEN LOWER(vehicle_type_code2) LIKE '%tanker%' THEN "truck"
  WHEN LOWER(vehicle_type_code2) LIKE '%suv%' THEN "car"
  WHEN LOWER(vehicle_type_code2) LIKE '%large com%' THEN "truck"
  WHEN LOWER(vehicle_type_code2) LIKE '%small com%' THEN "car"
  WHEN LOWER(vehicle_type_code2) LIKE '%unknown%' THEN "other"
  WHEN LOWER(vehicle_type_code2) LIKE '%wagon%' THEN "car"
  WHEN LOWER(vehicle_type_code2) LIKE '%limo%' THEN "car"
  WHEN LOWER(vehicle_type_code2) LIKE '%bike%' THEN "bicycle"
  WHEN LOWER(vehicle_type_code2) LIKE '%e bik%' THEN "e-scooter"
  WHEN LOWER(vehicle_type_code2) LIKE '%flat%' THEN "truck"
  WHEN LOWER(vehicle_type_code2) LIKE '%truck%' THEN "truck"
  WHEN LOWER(vehicle_type_code2) LIKE '%amb%' THEN "ambulance"
  WHEN LOWER(vehicle_type_code2) LIKE '%scoot%' THEN "scooter"
  WHEN LOWER(vehicle_type_code2) LIKE '%bus%' THEN "bus"
  WHEN LOWER(vehicle_type_code2) LIKE '%taxi%' THEN "taxi"
  WHEN LOWER(vehicle_type_code2) LIKE '%van%' THEN "van"
  WHEN LOWER(vehicle_type_code2) LIKE '%cab%' THEN "taxi"
  WHEN LOWER(vehicle_type_code2) LIKE '%motor%' THEN "scooter"
  WHEN LOWER(vehicle_type_code2) LIKE '%dump%' THEN "truck"
  WHEN LOWER(vehicle_type_code2) LIKE '%camper%' THEN "van"
  WHEN LOWER(vehicle_type_code2) LIKE '%carry%' THEN "other"
  WHEN LOWER(vehicle_type_code2) LIKE '%cycle%' THEN "bicycle"
  WHEN LOWER(vehicle_type_code2) LIKE '%covert%' THEN "car"
  WHEN LOWER(vehicle_type_code2) LIKE '%garb%' THEN "truck"
  WHEN LOWER(vehicle_type_code2) LIKE '%mop%' THEN "scooter"
  WHEN LOWER(vehicle_type_code2) IS NULL THEN NULL
  ELSE "other"
  END as new_vehicle2, 
CASE 
  WHEN LOWER(vehicle_type_code_3) LIKE '%sedan%' THEN "car"
  WHEN LOWER(vehicle_type_code_3) LIKE '%3-door%' THEN "car"
  WHEN LOWER(vehicle_type_code_3) LIKE '%vehicle%' THEN "car"
  WHEN LOWER(vehicle_type_code_3) LIKE '%e-scooter%' THEN "e-scooter"
  WHEN LOWER(vehicle_type_code_3) LIKE '%e-bike%' THEN "e-scooter"
  WHEN LOWER(vehicle_type_code_3) LIKE '%pk%' THEN "other"
  WHEN LOWER(vehicle_type_code_3) LIKE '%tanker%' THEN "truck"
  WHEN LOWER(vehicle_type_code_3) LIKE '%suv%' THEN "car"
  WHEN LOWER(vehicle_type_code_3) LIKE '%large com%' THEN "truck"
  WHEN LOWER(vehicle_type_code_3) LIKE '%small com%' THEN "car"
  WHEN LOWER(vehicle_type_code_3) LIKE '%unknown%' THEN "other"
  WHEN LOWER(vehicle_type_code_3) LIKE '%wagon%' THEN "car"
  WHEN LOWER(vehicle_type_code_3) LIKE '%limo%' THEN "car"
  WHEN LOWER(vehicle_type_code_3) LIKE '%bike%' THEN "bicycle"
  WHEN LOWER(vehicle_type_code_3) LIKE '%e bik%' THEN "e-scooter"
  WHEN LOWER(vehicle_type_code_3) LIKE '%flat%' THEN "truck"
  WHEN LOWER(vehicle_type_code_3) LIKE '%truck%' THEN "truck"
  WHEN LOWER(vehicle_type_code_3) LIKE '%amb%' THEN "ambulance"
  WHEN LOWER(vehicle_type_code_3) LIKE '%scoot%' THEN "scooter"
  WHEN LOWER(vehicle_type_code_3) LIKE '%bus%' THEN "bus"
  WHEN LOWER(vehicle_type_code_3) LIKE '%taxi%' THEN "taxi"
  WHEN LOWER(vehicle_type_code_3) LIKE '%van%' THEN "van"
  WHEN LOWER(vehicle_type_code_3) LIKE '%cab%' THEN "taxi"
  WHEN LOWER(vehicle_type_code_3) LIKE '%motor%' THEN "scooter"
  WHEN LOWER(vehicle_type_code_3) LIKE '%dump%' THEN "truck"
  WHEN LOWER(vehicle_type_code_3) LIKE '%camper%' THEN "van"
  WHEN LOWER(vehicle_type_code_3) LIKE '%carry%' THEN "other"
  WHEN LOWER(vehicle_type_code_3) LIKE '%cycle%' THEN "bicycle"
  WHEN LOWER(vehicle_type_code_3) LIKE '%covert%' THEN "car"
  WHEN LOWER(vehicle_type_code_3) LIKE '%garb%' THEN "truck"
  WHEN LOWER(vehicle_type_code_3) LIKE '%mop%' THEN "scooter"
  WHEN LOWER(vehicle_type_code_3) IS NULL THEN NULL
  ELSE "other"
  END as new_vehicle3,
CASE 
  WHEN LOWER(vehicle_type_code_4) LIKE '%sedan%' THEN "car"
  WHEN LOWER(vehicle_type_code_4) LIKE '%3-door%' THEN "car"
  WHEN LOWER(vehicle_type_code_4) LIKE '%vehicle%' THEN "car"
  WHEN LOWER(vehicle_type_code_4) LIKE '%e-scooter%' THEN "e-scooter"
  WHEN LOWER(vehicle_type_code_4) LIKE '%e-bike%' THEN "e-scooter"
  WHEN LOWER(vehicle_type_code_4) LIKE '%pk%' THEN "other"
  WHEN LOWER(vehicle_type_code_4) LIKE '%tanker%' THEN "truck"
  WHEN LOWER(vehicle_type_code_4) LIKE '%suv%' THEN "car"
  WHEN LOWER(vehicle_type_code_4) LIKE '%large com%' THEN "truck"
  WHEN LOWER(vehicle_type_code_4) LIKE '%small com%' THEN "car"
  WHEN LOWER(vehicle_type_code_4) LIKE '%unknown%' THEN "other"
  WHEN LOWER(vehicle_type_code_4) LIKE '%wagon%' THEN "car"
  WHEN LOWER(vehicle_type_code_4) LIKE '%limo%' THEN "car"
  WHEN LOWER(vehicle_type_code_4) LIKE '%bike%' THEN "bicycle"
  WHEN LOWER(vehicle_type_code_4) LIKE '%e bik%' THEN "e-scooter"
  WHEN LOWER(vehicle_type_code_4) LIKE '%flat%' THEN "truck"
  WHEN LOWER(vehicle_type_code_4) LIKE '%truck%' THEN "truck"
  WHEN LOWER(vehicle_type_code_4) LIKE '%amb%' THEN "ambulance"
  WHEN LOWER(vehicle_type_code_4) LIKE '%scoot%' THEN "scooter"
  WHEN LOWER(vehicle_type_code_4) LIKE '%bus%' THEN "bus"
  WHEN LOWER(vehicle_type_code_4) LIKE '%taxi%' THEN "taxi"
  WHEN LOWER(vehicle_type_code_4) LIKE '%van%' THEN "van"
  WHEN LOWER(vehicle_type_code_4) LIKE '%cab%' THEN "taxi"
  WHEN LOWER(vehicle_type_code_4) LIKE '%motor%' THEN "scooter"
  WHEN LOWER(vehicle_type_code_4) LIKE '%dump%' THEN "truck"
  WHEN LOWER(vehicle_type_code_4) LIKE '%camper%' THEN "van"
  WHEN LOWER(vehicle_type_code_4) LIKE '%carry%' THEN "other"
  WHEN LOWER(vehicle_type_code_4) LIKE '%cycle%' THEN "bicycle"
  WHEN LOWER(vehicle_type_code_4) LIKE '%covert%' THEN "car"
  WHEN LOWER(vehicle_type_code_4) LIKE '%garb%' THEN "truck"
  WHEN LOWER(vehicle_type_code_4) LIKE '%mop%' THEN "scooter"
  WHEN LOWER(vehicle_type_code_4) IS NULL THEN NULL
  ELSE "other"
  END as new_vehicle4,
CASE 
  WHEN LOWER(vehicle_type_code_5) LIKE '%sedan%' THEN "car"
  WHEN LOWER(vehicle_type_code_5) LIKE '%3-door%' THEN "car"
  WHEN LOWER(vehicle_type_code_5) LIKE '%vehicle%' THEN "car"
  WHEN LOWER(vehicle_type_code_5) LIKE '%e-scooter%' THEN "e-scooter"
  WHEN LOWER(vehicle_type_code_5) LIKE '%e-bike%' THEN "e-scooter"
  WHEN LOWER(vehicle_type_code_5) LIKE '%pk%' THEN "other"
  WHEN LOWER(vehicle_type_code_5) LIKE '%tanker%' THEN "truck"
  WHEN LOWER(vehicle_type_code_5) LIKE '%suv%' THEN "car"
  WHEN LOWER(vehicle_type_code_5) LIKE '%large com%' THEN "truck"
  WHEN LOWER(vehicle_type_code_5) LIKE '%small com%' THEN "car"
  WHEN LOWER(vehicle_type_code_5) LIKE '%unknown%' THEN "other"
  WHEN LOWER(vehicle_type_code_5) LIKE '%wagon%' THEN "car"
  WHEN LOWER(vehicle_type_code_5) LIKE '%limo%' THEN "car"
  WHEN LOWER(vehicle_type_code_5) LIKE '%bike%' THEN "bicycle"
  WHEN LOWER(vehicle_type_code_5) LIKE '%e bik%' THEN "e-scooter"
  WHEN LOWER(vehicle_type_code_5) LIKE '%flat%' THEN "truck"
  WHEN LOWER(vehicle_type_code_5) LIKE '%truck%' THEN "truck"
  WHEN LOWER(vehicle_type_code_5) LIKE '%amb%' THEN "ambulance"
  WHEN LOWER(vehicle_type_code_5) LIKE '%scoot%' THEN "scooter"
  WHEN LOWER(vehicle_type_code_5) LIKE '%bus%' THEN "bus"
  WHEN LOWER(vehicle_type_code_5) LIKE '%taxi%' THEN "taxi"
  WHEN LOWER(vehicle_type_code_5) LIKE '%van%' THEN "van"
  WHEN LOWER(vehicle_type_code_5) LIKE '%cab%' THEN "taxi"
  WHEN LOWER(vehicle_type_code_5) LIKE '%motor%' THEN "scooter"
  WHEN LOWER(vehicle_type_code_5) LIKE '%dump%' THEN "truck"
  WHEN LOWER(vehicle_type_code_5) LIKE '%camper%' THEN "van"
  WHEN LOWER(vehicle_type_code_5) LIKE '%carry%' THEN "other"
  WHEN LOWER(vehicle_type_code_5) LIKE '%cycle%' THEN "bicycle"
  WHEN LOWER(vehicle_type_code_5) LIKE '%covert%' THEN "car"
  WHEN LOWER(vehicle_type_code_5) LIKE '%garb%' THEN "truck"
  WHEN LOWER(vehicle_type_code_5) LIKE '%mop%' THEN "scooter"
  WHEN LOWER(vehicle_type_code_5) IS NULL THEN NULL
  ELSE "other"
  END as new_vehicle5
FROM `midyear-glazing-366016.nypd_version2.clean_zip`)
SELECT *
FROM clean_vehicle

#UPDATE FACTORS

WITH fac_list as(
SELECT contributing_factor_vehicle_1 as factor1, count(*) as count1 FROM `midyear-glazing-366016.nypd_version2.clean_zip`
where contributing_factor_vehicle_1 is not null and contributing_factor_vehicle_1 != 'Unspecified'
group by contributing_factor_vehicle_1
UNION all
select contributing_factor_vehicle_2 as factor1, count(*) as count1 FROM `midyear-glazing-366016.nypd_version2.clean_zip`
where contributing_factor_vehicle_2 is not null and contributing_factor_vehicle_2 != 'Unspecified'
group by contributing_factor_vehicle_2
UNION ALL
SELECT contributing_factor_vehicle_3 as factor1, count(*) as count1 FROM `midyear-glazing-366016.nypd_version2.clean_zip`
where contributing_factor_vehicle_3 is not null and contributing_factor_vehicle_3 != 'Unspecified'
group by contributing_factor_vehicle_3
UNION ALL
SELECT contributing_factor_vehicle_4 as factor1, count(*) as count1 FROM `midyear-glazing-366016.nypd_version2.clean_zip`
where contributing_factor_vehicle_4 is not null and contributing_factor_vehicle_4 != 'Unspecified'
group by contributing_factor_vehicle_4
UNION ALL
SELECT contributing_factor_vehicle_5 as factor1, count(*) as count1 FROM `midyear-glazing-366016.nypd_version2.clean_zip`
where contributing_factor_vehicle_5 is not null and contributing_factor_vehicle_5 != 'Unspecified'
group by contributing_factor_vehicle_5
)

SELECT fac_list.factor1,SUM(fac_list.count1) as sum from fac_list
group by fac_list.factor1
order by sum desc



#BOROUGH COLLISIONS

SELECT borough,
CASE 
  WHEN borough = 'BRONX' THEN ROUND(COUNT(*)/1427570*1000000) 
  WHEN borough = 'BROOKLYN' THEN  ROUND(COUNT(*)/2627275*1000000)
  WHEN borough = 'MANHATTAN' THEN  ROUND(COUNT(*)/1619000*1000000)
  WHEN borough = 'QUEENS' THEN ROUND(COUNT(*)/2322443*1000000)
   WHEN borough = 'STATEN ISLAND' THEN ROUND(COUNT(*)/485990*1000000)
  ELSE 0 
  END AS number_of_collisions
FROM `midyear-glazing-366016.nypd_version2.clean_vehicle_clean_borough` 
GROUP BY borough 

#CASUALTIES BY VEHICLES

WITH vehicle as(
SELECT new_vehicle1 as vehiclex, SUM(number_of_persons_injured) AS injury, SUM(number_of_persons_killed) AS death FROM `midyear-glazing-366016.nypd_version2.clean_vehicle_clean_borough` 
where new_vehicle1 is not null 
group by new_vehicle1
UNION all
select new_vehicle2 as vehiclex, SUM(number_of_persons_injured) AS injury, SUM(number_of_persons_killed) AS death FROM `midyear-glazing-366016.nypd_version2.clean_vehicle_clean_borough`
where new_vehicle2 is not null 
group by new_vehicle2
UNION ALL
SELECT new_vehicle3 as vehiclex, SUM(number_of_persons_injured) AS injury, SUM(number_of_persons_killed) AS death FROM `midyear-glazing-366016.nypd_version2.clean_vehicle_clean_borough`
where new_vehicle3 is not null 
group by new_vehicle3
UNION ALL
SELECT new_vehicle4 as vehiclex, SUM(number_of_persons_injured) AS injury, SUM(number_of_persons_killed) AS death FROM `midyear-glazing-366016.nypd_version2.clean_vehicle_clean_borough`
where new_vehicle4 is not null 
group by new_vehicle4
UNION ALL
SELECT new_vehicle5 as vehiclex, SUM(number_of_persons_injured) AS injury, SUM(number_of_persons_killed) AS death FROM `midyear-glazing-366016.nypd_version2.clean_vehicle_clean_borough`
where new_vehicle5 is not null 
group by new_vehicle5
)
SELECT vehiclex, ROUND(SUM(injury)/1756549*100,6) AS rate_of_injury, ROUND(SUM(death)/1756549*100,6) AS rate_of_death
FROM vehicle
group by vehicle.vehiclex
order by 2 DESC, 3 DESC 

#CASUALTIES BY PEDESTRIANS/MOTORISTS/CYCLISTS

SELECT ROUND(SUM(number_of_cyclist_injured)/SUM(number_of_persons_injured)*100,4) AS cyclist_injury, 
  ROUND(SUM(number_of_cyclist_killed)/SUM(number_of_persons_killed)*100,4) AS cyclist_death, 
  ROUND(SUM(number_of_motorist_injured)/SUM(number_of_persons_injured)*100,6) AS motorist_injury, 
  ROUND(SUM(number_of_motorist_killed)/SUM(number_of_persons_killed)*100,6) AS motorist_death , 
  ROUND(SUM(number_of_pedestrians_injured)/SUM(number_of_persons_injured)*100,6) AS pedestrian_injury, 
  ROUND(SUM(number_of_pedestrians_killed)/SUM(number_of_persons_killed)*100,6) AS pedestrian_death 
FROM `midyear-glazing-366016.nypd_version2.clean_vehicle_clean_borough` 
LIMIT 1000

#INTERSECTION

ALTER TABLE `midyear-glazing-366016.nypd_version2.clean_vehicle_clean_borough`
ADD COLUMN intersection BOOL;

update `midyear-glazing-366016.nypd_version2.clean_vehicle_clean_borough`
set intersection = true
where cross_street_name is not null and on_street_name is not null and off_street_name is null

update `midyear-glazing-366016.nypd_version2.clean_vehicle_clean_borough`
set intersection = false
where cross_street_name is null and on_street_name is  null and off_street_name is not null

update `midyear-glazing-366016.nypd_version2.clean_vehicle_clean_borough`
set intersection = true
where on_street_name is not null and off_street_name is null

ALTER TABLE `midyear-glazing-366016.nypd_version2.clean_vehicle_clean_borough`
drop COLUMN zip_code;

ALTER TABLE `midyear-glazing-366016.nypd_version2.clean_vehicle_clean_borough`
drop column location_1;

with find_null as (select new_borough, on_street_name, cross_street_name, location, count(*) as count_int
from `midyear-glazing-366016.nypd_version2.clean_vehicle_clean_borough`
where intersection is true and (LOWER(on_street_name) NOT LIKE '%parkway%' AND LOWER(on_street_name) NOT LIKE '%expressway%'AND LOWER(on_street_name) NOT LIKE '%pkwy%'AND LOWER(on_street_name) NOT LIKE '%expwy%') 
group by new_borough, on_street_name, cross_street_name, location 
order by count_int desc)
select * from find_null where cross_street_name is null and on_street_name is null

with find_null2 as(
select new_borough, on_street_name, cross_street_name, location, unique_key,new_vehicle1,new_vehicle2, contributing_factor_vehicle_1, contributing_factor_vehicle_2
from `midyear-glazing-366016.nypd_version2.clean_vehicle_clean_borough`
where intersection is true and (LOWER(on_street_name) NOT LIKE '%parkway%' AND LOWER(on_street_name) NOT LIKE '%expressway%'AND LOWER(on_street_name) NOT LIKE '%pkwy%'AND LOWER(on_street_name) NOT LIKE '%expwy%'))
select * from find_null2
where location is null 


#ARIMA FORECASTING

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








