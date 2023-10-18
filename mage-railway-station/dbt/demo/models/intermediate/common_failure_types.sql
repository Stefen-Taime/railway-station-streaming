SELECT 
  equipment,
  COUNT(*) AS failure_count
FROM 
  {{ ref('stg_equipment_failures') }}
GROUP BY 
  equipment
ORDER BY 
  failure_count DESC
