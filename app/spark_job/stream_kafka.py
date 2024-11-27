import yaml
import findspark
from pyspark.sql import SparkSession
import os
import sys
# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
from utils.kafka_to_delta import *
from utils.query_definations import *

findspark.init()




# Load configuration from config.yaml
def load_config(config_file="config/config.yaml"):
    with open(config_file, 'r') as file:
        return yaml.safe_load(file)



# Initialize logging based on the config
def init_logging(log_level, log_file):
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
    )


# Load the config
config = load_config()

# # Set up logging
# init_logging(config["logging"]["level"], config["logging"]["log_file"])

# Kafka configuration
kafka_broker = config["kafka"]["broker"]

# Define schemas
# Updated schema where Segment is correctly defined as an array of structs
transaction_schema_for_delta = StructType([
    StructField("UniqueId", StringType(), True),
    StructField("TransactionDateUTC", StringType(), True),
    StructField("Itinerary", StringType(), True),
    StructField("OriginAirportCode", StringType(), True),
    StructField("DestinationAirportCode", StringType(), True),
    StructField("OneWayOrReturn", StringType(), True),
    StructField("Segment", ArrayType(StructType([
        StructField("DepartureAirportCode", StringType(), True),
        StructField("ArrivalAirportCode", StringType(), True),
        StructField("SegmentNumber", StringType(), True),
        StructField("LegNumber", StringType(), True),
        StructField("NumberOfPassengers", StringType(), True)
    ])), True)
])

location_schema_for_delta = StructType([
    StructField("AirportCode", StringType(), True),
    StructField("CountryName", StringType(), True),
    StructField("Region", StringType(), True)
])

os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--jars jars/delta-core_2.12-2.4.0.jar '
    '--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension '
    '--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog '
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,'
    'io.delta:delta-storage:2.4.0,'
    'org.apache.spark:spark-sql_2.12:3.4.0,'
    'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.0 '
    'pyspark-shell'
)


# Kafka topics and paths from config
transaction_schema_path = config["schema"]["transaction_schema_path"]
location_schema_path = config["schema"]["location_schema_path"]
transaction_delta_path = config["delta"]["transaction_table_path"]
location_delta_path = config["delta"]["location_table_path"]
checkpoint_path = config["delta"]["checkpoint_path"]
delta_table_path = config["delta"]["delta_table_path"]


def run_spark_job():
    # Initialize Spark session
    logging.info("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("KafkaToDelta") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
        .config("spark.sql.debug.maxToStringFields", 10000) \
        .getOrCreate()
    logging.info("Spark session started successfully.")

    # # Load schemas
    # location_schema = load_schema(location_schema_path)
    # transaction_schema = load_schema(transaction_schema_path)

    # # Map topics to their respective schemas
    # spark_schema_map = {
    #     "Location": location_schema,
    #     "Transaction": transaction_schema
    # }

    schema_map = {
        "Location": location_schema_for_delta,
        "Transaction": transaction_schema_for_delta
    }

    # List of Kafka topics to subscribe to
    kafka_topics = ["Location", "Transaction"]

    # Start the stream from Kafka topics
    query = _start_kafka_stream(
        session=spark,
        log=spark._jvm.org.apache.log4j.LogManager.getLogger("KafkaToDelta"),
        schema_map=schema_map,
        # spark_schema_map = spark_schema_map,# Map of topic to schema
        kafka_bootstrap_servers=kafka_broker,
        kafka_topics=kafka_topics,  # Multiple topics
        checkpoint_topic_path=checkpoint_path,
        output_path=delta_table_path,
        starting_offsets="earliest"
    )
    logging.info("Kafka stream started.")
    query.awaitTermination(timeout=120)
    return spark


def _start_kafka_stream(session, log, schema_map, kafka_bootstrap_servers, kafka_topics, checkpoint_topic_path, output_path,
                        starting_offsets="latest"):
    log.info(f"_start_kafka_stream == {kafka_topics}")

    # Read from multiple Kafka topics
    kafka_df = session.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", ",".join(kafka_topics)) \
        .option("startingOffsets", starting_offsets) \
        .load()

    # Stream data from each topic separately and write it to Delta table
    for topic in kafka_topics:
        schema = schema_map.get(topic)
        if schema:
            # Filter the Kafka stream based on the topic and parse the message according to the schema
            topic_df = parse_kafka_message(kafka_df, schema_map.get(topic))

            # If the topic is "Transaction", flatten the data before writing to Delta
            if topic == "Transaction":
                topic_df = flatten_transaction_data(topic_df)

            # Check if the DataFrame is streaming
            print(f"Is the DataFrame streaming? {topic_df.isStreaming}")

            # Write data from the topic into Delta
            query = topic_df.writeStream \
                .format("delta") \
                .outputMode("append") \
                .option("checkpointLocation", f"{checkpoint_topic_path}/{topic}") \
                .start(f"{output_path}/{topic}")  # You can have separate directories per topic

    return query


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    location_delta_path = "app/delta_table/Location"
    transaction_delta_path = "app/delta_table/Transaction"
    query3_path = "utils/"
    spark = run_spark_job()
    read_delta_table(spark, location_delta_path, num_rows=1000)
    #get_country_with_most_transactions(spark, location_delta_path, transaction_delta_path)
    #analyze_and_visualize_transaction_split(transaction_delta_path, location_delta_path, spark)
    visualize_segment_distribution(transaction_delta_path, spark, query3_path)
