SELECT
  w.condition AS weather_condition,
  ROUND(AVG(f.tip_amount / NULLIF(f.fare_amount, 0)) * 100, 2) AS avg_tip_percentage
FROM
  {{ ref('fact_taxi_trips_parquet') }} f
JOIN
  {{ ref('dim_weather_parquet') }} w
    ON DATE_TRUNC('hour', f.tpep_pickup_datetime) = DATE_TRUNC('hour', w.datetime)
GROUP BY
  w.condition
ORDER BY
  avg_tip_percentage DESC
