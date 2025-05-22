-- models/dim_weather_parquet.sql

SELECT
    weather_id,
    datetime,
    city,
    -- location, -- ⛔️ on le commente car pas structuré
    temperature,
    feels_like,
    humidity,
    wind_speed,
    condition,
    pressure,
    precipitation,
    visibility
FROM read_parquet('../data/weather_parquet/*.parquet')