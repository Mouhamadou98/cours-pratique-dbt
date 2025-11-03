-- SELECT passenger_count, Airport_fee, fare_amount, RatecodeID, store_and_fwd_flag
-- FROM {{ source('tlc_taxi_trips', 'raw_yellow_trips_data') }}
-- LIMIT 10

-- ATTACH 'output/transformed_data.db' as transformed_db;
--  select * from transformed_db.transform;

-- CTE pour charger les données sources, CTE c'est Common Table Expression qui permet de structurer une requête SQL en plusieurs étapes
{{ config(
    materialized='external',
    location='output/trips_2024_transformed.parquet',
    format='parquet'
) }}

WITH sources_data AS (
    SELECT 
        * EXCLUDE(VendorID, RatecodeID) 
    FROM 
        {{ source('tlc_taxi_trips', 'raw_yellow_trips_data') }}
),

filtered_data AS (
    SELECT *
    FROM sources_data
    WHERE 
        passenger_count > 0 -- Exclure les trajets avec 0 passager
        AND trip_distance > 0 -- Exclure les trajets avec distance négative ou nulle
        AND total_amount > 0 -- Exclure les trajets avec tarif négatif ou nul
        AND tpep_dropoff_datetime > tpep_pickup_datetime -- Exclure les trajets avec durée négative ou nulle
        AND store_and_fwd_flag = 'N' -- Exclure les trajets payés en espèces
        AND tip_amount >= 0 -- Exclure les trajets avec pourboire négatif
        AND payment_type IN (1, 2) -- Garder les trajets payés par carte de crédit ou par espèces
),

transformed_data AS (
    SELECT 
        CAST(passenger_count AS BIGINT) AS passenger_count,

        CASE 
            WHEN payment_type = 1 THEN 'Credit Card'
            WHEN payment_type = 2 THEN 'Cash'
        END AS payment_method,

        DATE_DIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes,

        * EXCLUDE(passenger_count, payment_type)
        
    FROM 
        filtered_data
),

final_data AS (
    SELECT *,
        CAST(tpep_pickup_datetime AS DATE) as pickup_data,
        CAST(tpep_dropoff_datetime AS DATE) as dropoff_data
    FROM transformed_data
    WHERE 
        pickup_data >= DATE '2024-01-01'
        AND dropoff_data < DATE '2024-02-01'
)

SELECT * EXCLUDE(pickup_data, dropoff_data)
FROM final_data
WHERE trip_duration_minutes > 0 -- Exclure les trajets avec durée négative ou nulle après transformation