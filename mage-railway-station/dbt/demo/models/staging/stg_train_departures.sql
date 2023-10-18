-- staging/stg_train_departures.sql

SELECT
    id,
    station,
    CAST(timestamp AS TIMESTAMP) AS departure_timestamp,
    CAST(passengers AS INT64) AS passengers,
    CAST(dock_number AS INT64) AS dock_number,
    CAST(wagon_count AS INT64) AS wagon_count,
    direction
FROM 
    {{ source('staging_data', 'train_departures') }}
WHERE
    id IS NOT NULL
    AND station IS NOT NULL
    AND timestamp IS NOT NULL
    AND passengers IS NOT NULL
    AND dock_number IS NOT NULL
    AND wagon_count IS NOT NULL
    AND direction IS NOT NULL
