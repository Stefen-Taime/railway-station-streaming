WITH 
total_validations AS (
    -- Count the total number of records in stg_ticket_validations
    SELECT COUNT(*) as count FROM {{ ref('stg_ticket_validations') }}
),
rejection_rate AS (
    -- Directly reference the precomputed ticket_rejection_rate model
    SELECT * FROM {{ ref('ticket_rejection_rate') }}
),
validations_by_hour AS (
    -- Directly reference the precomputed ticket_validations_by_hour model
    SELECT * FROM {{ ref('ticket_validations_by_hour') }}
)

-- Aggregate the above metrics into a single view
SELECT 
    'Total Validations' AS metric_name,
    CAST(count AS STRING) AS metric_value
FROM total_validations

UNION ALL

SELECT 
    'Rejection Rate (%)' AS metric_name,
    CAST(rejection_rate_percentage AS STRING) AS metric_value
FROM rejection_rate

UNION ALL

SELECT 
    CONCAT('Validations @ Hour ', CAST(hour_of_day AS STRING)) AS metric_name,
    CAST(validations_in_hour AS STRING) AS metric_value
FROM validations_by_hour
