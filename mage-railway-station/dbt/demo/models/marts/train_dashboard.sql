-- mart/train_dashboard.sql

WITH total_departures AS (
    SELECT * FROM {{ ref('total_train_departures') }}
),
total_arrivals AS (
    SELECT * FROM {{ ref('total_train_arrivals') }}
)

SELECT 
    'Total Train Departures' AS metric_name,
    CAST(total_departures.total_departures AS STRING) AS metric_value
FROM total_departures

UNION ALL

SELECT 
    'Total Train Arrivals' AS metric_name,
    CAST(total_arrivals.total_arrivals AS STRING) AS metric_value
FROM total_arrivals
