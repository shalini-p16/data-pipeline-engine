import json
import logging
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructField, StringType, StructType, ArrayType


# Load configuration from config.yaml
def load_schema(schema_file_path):
    """Loads JSON schema from a file and converts it to a StructType schema."""
    with open(schema_file_path, 'r') as file:
        s = file.read()
        json_schema = json.loads(s)
        return create_spark_schema(json_schema)

# Initialize logging based on the config
def init_logging(log_level, log_file):
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
    )

# Function to flatten the transaction data
def flatten_transaction_data(df):
    """Flattens the transaction data."""

    # Explode the 'Segment' array into separate rows
    flattened_df = df.withColumn("Segment", explode(col("Segment")))

    # Extract the nested fields from the exploded 'Segment'
    flattened_df = flattened_df.select(
        "UniqueId",
        "TransactionDateUTC",
        "Itinerary",
        "OriginAirportCode",
        "DestinationAirportCode",
        "OneWayOrReturn",
        col("Segment.DepartureAirportCode").alias("DepartureAirportCode"),
        col("Segment.ArrivalAirportCode").alias("ArrivalAirportCode"),
        col("Segment.SegmentNumber").alias("SegmentNumber"),
        col("Segment.LegNumber").alias("LegNumber"),
        col("Segment.NumberOfPassengers").alias("NumberOfPassengers")
    )

    return flattened_df

# Function to parse the Kafka message to a structured format
def parse_kafka_message(kafka_df, schema):
    """Parse the Kafka message value to a structured format."""
    return kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), schema).alias("data")) \
        .select("data.*")

# Function to read data from a Delta table
def read_delta_table(spark, delta_table_path, num_rows=5):
    """Reads and prints the data from the Delta table."""
    if not spark:
        raise ValueError("SparkSession is not initialized.")
    print(f"Reading data from Delta table at {delta_table_path}...")
    delta_df = spark.read.format("delta").load(delta_table_path)
    delta_df.limit(num_rows).show(truncate=False)
    delta_df.printSchema()

# Function to create Spark schema from a JSON schema
def create_spark_schema(json_schema):
    """Converts a JSON schema to a Spark StructType."""
    fields = []
    for field_name, field_info in json_schema['properties'].items():
        spark_type = StringType()  # All fields are strings in this schema
        fields.append(StructField(field_name, spark_type, True))

    return StructType(fields)

def _start_kafka_stream(session, log, schema_map, kafka_bootstrap_servers, kafka_topics, checkpoint_path, output_path,
                        starting_offsets="latest"):
    log.info(f"_start_kafka_stream == {kafka_topics}")

    # Read from multiple Kafka topics
    kafka_df = session.readStream \
        .format(".bk") \
        .option(".bk.bootstrap.servers", kafka_bootstrap_servers) \
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

            # Write data from the topic into Delta (you can have separate directories per topic)
            query = topic_df.writeStream \
                .format("delta") \
                .outputMode("append") \
                .option("checkpointLocation", f"{checkpoint_path}/{topic}") \
                .start(f"{output_path}/{topic}")  # You can have separate directories per topic

    return query




