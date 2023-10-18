SELECT 
  COUNT(*) AS total_incidents
FROM 
  {{ ref('stg_incidents') }}
