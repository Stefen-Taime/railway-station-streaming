-- intermediate/total_train_arrivals.sql

SELECT COUNT(*) AS total_arrivals
FROM {{ source('staging_data', 'train_arrivals') }}
