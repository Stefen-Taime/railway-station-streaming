-- intermediate/total_train_departures.sql

SELECT COUNT(*) AS total_departures
FROM {{ source('staging_data', 'train_departures') }}
