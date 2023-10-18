from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Schema for incidents topic
incidents_schema = StructType([
  StructField("id", StringType(), True),
  StructField("incident_type", StringType(), True),
  StructField("station", StringType(), True),
  StructField("location", StringType(), True),
  StructField("timestamp", TimestampType(), True),
  StructField("severity", StringType(), True),
  StructField("duration", DoubleType(), True)
])

# Schema for equipment_failures topic 
equipment_failures_schema = StructType([
  StructField("id", StringType(), True),
  StructField("equipment", StringType(), True),
  StructField("station", StringType(), True),
  StructField("location", StringType(), True),
  StructField("timestamp", TimestampType(), True),  
  StructField("duration", DoubleType(), True)
])

# Schema for train_arrivals topic
train_arrivals_schema = StructType([
  StructField("id", StringType(), True),
  StructField("station", StringType(), True),
  StructField("timestamp", TimestampType(), True),
  StructField("passengers", IntegerType(), True),
  StructField("dock_number", IntegerType(), True),
  StructField("wagon_count", IntegerType(), True),
  StructField("direction", StringType(), True)  
])

# Schema for train_departures topic
train_departures_schema = StructType([
  StructField("id", StringType(), True),
  StructField("station", StringType(), True),
  StructField("timestamp", TimestampType(), True),
  StructField("passengers", IntegerType(), True),
  StructField("dock_number", IntegerType(), True),
  StructField("wagon_count", IntegerType(), True),
  StructField("direction", StringType(), True)  
])

# Schema for ticket_validations topic
ticket_validations_schema = StructType([
  StructField("id", StringType(), True),
  StructField("train_id", StringType(), True),
  StructField("station", StringType(), True),
  StructField("timestamp", TimestampType(), True),
  StructField("validations", IntegerType(), True)  
])

# Schema for passenger_flow topic
passenger_flow_schema = StructType([
  StructField("id", StringType(), True),
  StructField("train_id", StringType(), True),
  StructField("station", StringType(), True),
  StructField("timestamp", TimestampType(), True), 
  StructField("waiting_timestamp", DoubleType(), True),
  StructField("ticket_check_timestamp", DoubleType(), True),
  StructField("reduced_flow_factor", DoubleType(), True)
])
