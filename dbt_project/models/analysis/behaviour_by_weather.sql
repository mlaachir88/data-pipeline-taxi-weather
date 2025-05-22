-- models/analysis/behaviour_by_weather.sql
SELECT
  w.condition AS weather_condition,
  COUNT(*) AS total_trips,
  ROUND(AVG(f.trip_duration_min), 2) AS avg_duration,
  ROUND(AVG(f.tip_amount / NULLIF(f.fare_amount, 0)) * 100, 2) AS avg_tip_percentage
FROM
  {{ ref('fact_taxi_trips_parquet') }} f
JOIN
  {{ ref('dim_weather_parquet') }} w
    ON DATE_TRUNC('hour', f.tpep_pickup_datetime) = DATE_TRUNC('hour', w.datetime)
GROUP BY
  w.condition
ORDER BY
  total_trips DESC