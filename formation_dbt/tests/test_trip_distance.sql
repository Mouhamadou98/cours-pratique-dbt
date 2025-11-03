SELECT *
FROM {{ ref('transform') }}
WHERE 
    trip_distance IS NULL 
    OR trip_distance <= 0