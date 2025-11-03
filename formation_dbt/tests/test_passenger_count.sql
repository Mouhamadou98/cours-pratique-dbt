SELECT *
FROM {{ ref('transform') }}
WHERE 
    passenger_count IS NULL 
    OR passenger_count <= 0
    OR passenger_count != CAST(passenger_count AS BIGINT)