SELECT 
  station,
  COUNT(*) AS failure_count
FROM 
  {{ ref('stg_equipment_failures') }}
GROUP BY 
  station
ORDER BY 
  failure_count DESC
LIMIT  4
