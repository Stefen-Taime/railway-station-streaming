from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import traceback
import logging
from mapping import *
import time

CONFIG_FILE_PATH = "/home/stefen/client.properties"
LOGGING_LEVEL = logging.INFO

class KafkaStreamProcessor:
    def __init__(self, config_path):
        self.props = self.read_ccloud_config(config_path)
        self.kafka_params = {
            "kafka.bootstrap.servers": self.props.get("bootstrap.servers"),
            "kafka.security.protocol": self.props.get("security.protocol", "PLAINTEXT"),
            "kafka.sasl.mechanism": self.props.get("sasl.mechanism", "PLAIN"),
            "kafka.sasl.jaas.config": self.props.get("sasl.jaas.config"),
            "kafka.max.poll.interval.ms": "300000",
            "kafka.max.poll.records": "200"
        }

    def read_ccloud_config(self, filepath):
        logger.info(f'Reading Confluent Cloud configuration from {filepath}')
        conf_dict = {}
        try:
            with open(filepath, 'r') as file:
                for line in file:
                    if line[0] != '#' and '=' in line:
                        key, value = line.strip().split('=', 1)
                        conf_dict[key] = value
            logger.info(f'Successfully read {len(conf_dict)} configurations from {filepath}')
        except Exception as e:
            logger.error(f'Failed to read configuration from {filepath}: {e}')
            logger.error(traceback.format_exc())
        return conf_dict

    @staticmethod
    def create_or_get_spark_session(app_name, master="yarn"):
        spark = (SparkSession.builder.appName(app_name).master(master).getOrCreate())
        return spark

    def create_kafka_read_stream(self, spark, topic, starting_offset="earliest"):
        read_stream = (spark.readStream.format("kafka").options(**self.kafka_params)
                       .option("failOnDataLoss", False).option("startingOffsets", starting_offset)
                       .option("subscribe", topic).load())
        return read_stream

    @staticmethod
    def process_stream(stream, stream_schema):
        stream = (stream.selectExpr("CAST(value AS STRING)")
                .where(col("value").isNotNull())  # Filter out null values
                .where(col("value") != "")  # Filter out empty strings
                .select(from_json(col("value"), stream_schema).alias("data"))
                .select("data.*").withColumnRenamed("time", "timestamp"))
        stream = stream.withColumn("timestamp", (col("timestamp").cast("long") / 1000).cast("timestamp"))

        return stream

    @staticmethod
    def create_file_write_stream(stream, storage_path, checkpoint_path, trigger="180 seconds", output_mode="append", file_format="parquet"):
        write_stream = (stream.writeStream.format(file_format).option("path", storage_path)
                        .option("checkpointLocation", checkpoint_path).trigger(processingTime=trigger).outputMode(output_mode))
        return write_stream

    @staticmethod
    def process_each_batch(batch_df, batch_id):
        if batch_df.count() == 0:
            logger.info(f"Batch ID: {batch_id} est vide. Ignorer le traitement.")
            return
   
    def run(self):
        logger.info('Starting main function')
        try:
            spark = self.create_or_get_spark_session("KafkaToGCS")
            logger.info('Spark session created or retrieved')

            topic_schema_mapping = {
                "incidents": incidents_schema,
                "equipment_failures": equipment_failures_schema,
                "train_arrivals": train_arrivals_schema,
                "train_departures": train_departures_schema,
                "ticket_validations": ticket_validations_schema,
                "passenger_flow": passenger_flow_schema
            }

            for topic, schema in topic_schema_mapping.items():
                stream = self.create_kafka_read_stream(spark, topic)
                processed_stream = self.process_stream(stream, schema)
                write_stream = self.create_file_write_stream(
                    processed_stream, f"gs://railway_station/{topic}", f"/checkpoints/{topic}")
                write_stream.start()
            
            # Let's wait for a short duration for data to be available in memory table
            time.sleep(10)
            
            # Now, check the content of the in-memory table
            if "kafka_stream_debug" in spark.catalog.listTables():
                spark.sql("SELECT * FROM kafka_stream_debug").show(truncate=False)
            else:
                logger.warning("Table kafka_stream_debug is not yet available.")

            spark.streams.awaitAnyTermination()
            logger.info('Awaiting any stream termination')
        except Exception as e:
            logger.error(f'An error occurred in main function: {e}', exc_info=True)
        logger.info('Exiting main function')

if __name__ == "__main__":
    # Logger Configuration
    logging.basicConfig(level=LOGGING_LEVEL)
    logger = logging.getLogger(__name__)

    processor = KafkaStreamProcessor(CONFIG_FILE_PATH)
    processor.run()
