SELECT 
  id,
  equipment,
  station,
  location,
  TIMESTAMP(timestamp) AS failure_timestamp,
  duration
FROM 
  {{ source('staging_data', 'equipment_failures') }}
