SELECT 
  COUNT(*) AS total_failures
FROM 
  {{ ref('stg_equipment_failures') }}
