WITH total AS (
  SELECT * FROM {{ ref('total_incidents') }}
),
common_types AS (
  SELECT * FROM {{ ref('common_incident_types') }}
),
top_stations AS (
  SELECT * FROM {{ ref('stations_with_most_incidents') }}
)

SELECT
  'Total Incidents' AS metric_name,
  CAST(total_incidents AS STRING) AS metric_value
FROM total

UNION ALL

SELECT
  incident_type AS metric_name,
  CAST(incident_count AS STRING) AS metric_value
FROM common_types

UNION ALL

SELECT
  station AS metric_name,
  CAST(incident_count AS STRING) AS metric_value
FROM top_stations
