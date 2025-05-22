{{ config(materialized='table') }}

SELECT
    vendorid,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    payment_type,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    (DATE_DIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime)) AS trip_duration_min,
    CASE
        WHEN trip_distance <= 2 THEN '0-2km'
        WHEN trip_distance <= 5 THEN '2-5km'
        ELSE '>5km'
    END AS distance_category,
    (tip_amount / fare_amount) * 100 AS tip_percentage,
    HOUR(tpep_pickup_datetime) AS pickup_hour,
    STRFTIME(tpep_pickup_datetime, '%w') AS pickup_weekday
FROM read_parquet('../data/taxi_parquet/yellow_tripdata_2023-*.parquet')