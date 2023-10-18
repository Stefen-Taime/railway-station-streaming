WITH total AS (
  SELECT * FROM {{ ref('total_failures') }}
),
common_types AS (
  SELECT * FROM {{ ref('common_failure_types') }}
),
top_stations AS (
  SELECT * FROM {{ ref('stations_with_most_failures') }}
)

SELECT
  'Total Equipment Failures' AS metric_name,
  CAST(total_failures AS STRING) AS metric_value
FROM total

UNION ALL

SELECT
  equipment AS metric_name,
  CAST(failure_count AS STRING) AS metric_value
FROM common_types

UNION ALL

SELECT
  station AS metric_name,
  CAST(failure_count AS STRING) AS metric_value
FROM top_stations
