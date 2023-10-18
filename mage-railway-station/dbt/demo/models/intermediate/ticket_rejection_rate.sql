-- intermediate/ticket_rejection_rate.sql

WITH 
ticket_counts AS (
    SELECT
        COUNT(*) AS total_tickets,
        COUNTIF(CAST(validations AS INT) = 0) AS rejected_tickets
    FROM
       {{ ref('stg_ticket_validations') }}
)

SELECT
    rejected_tickets,
    total_tickets,
    (rejected_tickets * 1.0 / total_tickets) * 100 AS rejection_rate_percentage
FROM 
    ticket_counts
