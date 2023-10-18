SELECT 
  station,
  COUNT(*) AS incident_count
FROM 
  {{ ref('stg_incidents') }}
GROUP BY 
  station
ORDER BY 
  incident_count DESC
LIMIT 4 
