SELECT 
  incident_type,
  COUNT(*) AS incident_count
FROM 
  {{ ref('stg_incidents') }}
GROUP BY 
  incident_type
ORDER BY 
  incident_count DESC
