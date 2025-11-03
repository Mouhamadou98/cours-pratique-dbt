-- SELECT * from 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-11.parquet' LiMIT 10;

-- SELECT COUNT(*) FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet';

-- SELECT VendorID, 
--        COUNT(*) AS total_trips,
-- FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
-- GROUP BY VendorID;

-- SELECT RatecodeID, 
--        COUNT(*) AS total_trips,
-- FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
-- GROUP BY RatecodeID;

-- SELECT store_and_fwd_flag, 
--        COUNT(*) AS total_trips,
-- FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
-- GROUP BY store_and_fwd_flag;

-- SELECT PULocationID, 
--        COUNT(*) AS total_trips,
-- FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
-- GROUP BY PULocationID;

-- SELECT DOLocationID, 
--        COUNT(*) AS total_trips,
-- FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
-- GROUP BY DOLocationID;

-- .read './analyses/analyse_exploratoire.sql'

-- On peut vérifier les trajets dont la distance est nulle ou négative

-- On peut aussi regarder les trajets avec un tarif négatif ou nul

-- On peut aussi regarder les heures de trajets négatives ou nulles

-- DESCRIBE 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet';


SELECT 
    VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, PULocationID, DOLocationID, fare_amount
FROM 
    'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
WHERE 
    trip_distance <= 0 -- Trajet avec distance négative ou nulle
    OR fare_amount <= 0 -- Trajet avec tarif négatif ou nul
    OR tpep_dropoff_datetime <= tpep_pickup_datetime -- Trajet avec durée négative ou nulle
LIMIT 10;

-- selection d'exclusion de colonnes
SELECT * EXCLUDE(passenger_count, Airport_fee, fare_amount, RatecodeID, store_and_fwd_flag, payment_type, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge) 
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
LIMIT 10;