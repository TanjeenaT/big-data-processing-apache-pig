-- Task 1 - Load and Clean

-- loading files
trips = LOAD 'hdfs:///Input/Trips.txt' 
        USING PigStorage('\t')
        as (trip_id:int, taxi_id:int, company_id:int, dropoff_lat:double, dropoff_lon:double, distance_km:double, fare:double);

taxis = LOAD 'hdfs:///Input/Taxis.txt' 
        USING PigStorage('\t')
        as (taxi_id:int, license_plate:chararray, medallion_year:int, driver_rating:double);

companies = LOAD 'hdfs:///Input/Companies.txt' 
            USING PigStorage('\t')
            as (company_id:int, company_name:chararray);

-- data cleaning: 
-- 1. including only those trips where every required field contains a value
-- 2. removing outliers where trips --> distance_km <=0 (as wrong data) or distance_km >20 or fare <5

cleanedTrips = FILTER trips BY
    trip_id is not null AND taxi_id is not null AND company_id is not null AND
    dropoff_lat is not null AND dropoff_lon is not null AND
    distance_km is not null AND fare is not null AND
    distance_km > 0 AND distance_km <= 20 AND fare >= 5;

-- data storing cleaned trips 
STORE cleanedTrips INTO 'hdfs:///Output/clean_trips' USING PigStorage('\t');


-- Task 2 - Joins and Enrichment

clean_trips = LOAD 'hdfs:///Output/clean_trips' 
              USING PigStorage('\t')
              as (trip_id:int, taxi_id:int, company_id:int, dropoff_lat:double, dropoff_lon:double, distance_km:double, fare:double);

-- performing inner join cleaned trips to the taxis table on taxi_id
trips_taxis = JOIN clean_trips BY taxi_id, taxis BY taxi_id;

-- performing next join that intermediate results with companies on company_id
enriched_all = JOIN trips_taxis BY company_id, companies BY company_id;

-- making the final table
-- taxi_id, company_id, company_name, driver_rating, distance_km, fare, dropoff_lat, dropoff_lon
enriched_trips = FOREACH enriched_all GENERATE
    clean_trips::taxi_id      AS taxi_id,
    clean_trips::company_id   AS company_id,
    companies::company_name   AS company_name,
    taxis::driver_rating      AS driver_rating,
    clean_trips::distance_km  AS distance_km,
    clean_trips::fare         AS fare,
    clean_trips::dropoff_lat  AS dropoff_lat,
    clean_trips::dropoff_lon  AS dropoff_lon;

-- data storing enriched_trips
STORE enriched_trips INTO 'hdfs:///Output/enriched_trips' USING PigStorage('\t');


-- Task 3 - Aggregation

enrichedTrips = LOAD 'hdfs:///Output/enriched_trips'
                     USING PigStorage('\t')
                     as (taxi_id:int, company_id:int, company_name:chararray, driver_rating:double, distance_km:double, fare:double, dropoff_lat:double, dropoff_lon:double);

t3_grouped = GROUP enrichedTrips BY (company_id, company_name);

t3_stats = FOREACH t3_grouped GENERATE
    group.company_id                      AS company_id,
    group.company_name                    AS company_name,
    COUNT(enrichedTrips)             AS trip_count,
    SUM(enrichedTrips.distance_km)   AS total_distance_km_raw,
    AVG(enrichedTrips.distance_km)   AS avg_distance_km_raw,
    AVG(enrichedTrips.fare)          AS avg_fare_raw;

t3_final_stats = FOREACH t3_stats GENERATE
    company_id,
    company_name,
    trip_count,
    (ROUND(total_distance_km_raw * 100.0) / 100.0) AS total_distance_km,
    (ROUND(avg_distance_km_raw * 100.0) / 100.0)   AS avg_distance_km,
    (ROUND(avg_fare_raw * 100.0) / 100.0)          AS avg_fare;

-- sorting in ascending order by trip_count; if two companies have the same trip_count, break the tie by sorting on company_name
company_stats = ORDER t3_final_stats BY trip_count ASC, company_name ASC;  

-- data storing company_stats 
STORE company_stats INTO 'hdfs:///Output/company_stats' USING PigStorage('\t');


-- Task 4 - UDF for Fare Binning

-- step 1: registering the python UFD script
REGISTER 'fare_band.py' USING jython AS fb;

enrichedTrips_t4 = LOAD 'hdfs:///Output/enriched_trips'
                 USING PigStorage('\t')
                 as (taxi_id:int, company_id:int, company_name:chararray, driver_rating:float, distance_km:float, fare:float, dropoff_lat:float, dropoff_lon:float);


t4_fare_band = FOREACH enrichedTrips_t4 GENERATE
               company_id,
               company_name,
               (chararray)fb.fare_band(fare) AS fare_band;

-- data cleaning
t4_fare_band_clean = FILTER t4_fare_band BY fare_band IS NOT NULL;

-- aggregating by company_id and company_name
t4_agg = GROUP t4_fare_band_clean BY (company_id, company_name);

-- producing three fields - low_count, mid_count, and high_count
fare_bands_by_company = FOREACH t4_agg {
  low  = FILTER t4_fare_band_clean BY fare_band == 'LOW';
  mid  = FILTER t4_fare_band_clean BY fare_band == 'MID';
  high = FILTER t4_fare_band_clean BY fare_band == 'HIGH';

-- final counts of three fields
  GENERATE
    group.company_id   AS company_id,
    group.company_name AS company_name,
    COUNT(low)         AS low_count,
    COUNT(mid)         AS mid_count,
    COUNT(high)        AS high_count;
};

-- data storing fare_bands_by_company 
STORE fare_bands_by_company INTO 'hdfs:///Output/fare_bands_by_company' USING PigStorage('\t');