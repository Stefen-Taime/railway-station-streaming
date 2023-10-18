SELECT
    EXTRACT(HOUR FROM ticket_timestamp) AS hour_of_day,
    SUM(CAST(validations AS INT)) AS validations_in_hour
FROM
    {{ ref('stg_ticket_validations') }}
GROUP BY
    hour_of_day
ORDER BY
    hour_of_day
