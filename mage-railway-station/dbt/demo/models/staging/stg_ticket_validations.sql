SELECT
    id,
    train_id,
    station,
    CAST(timestamp AS TIMESTAMP) AS ticket_timestamp, 
    CAST(validations AS INT64) AS validations 
FROM 
    {{ source('staging_data', 'ticket_validations') }}
WHERE
    train_id IS NOT NULL 
    AND station IS NOT NULL 
    AND timestamp IS NOT NULL
    AND validations IS NOT NULL
