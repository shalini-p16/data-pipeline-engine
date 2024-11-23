import json
import logging
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructField, StringType, StructType, ArrayType, IntegerType
import findspark
from pyspark.sql import SparkSession
import os

findspark.init()

os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--jars file:///C:/Users/shali/OneDrive/Desktop/Travix/jar/delta-spark_2.12-3.2.0.jar '
    '--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" '
    '--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" '
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4,'  # Kafka for Scala 2.12
    'io.delta:delta-storage:3.2.0,'  # Delta Storage for Scala 2.12
    'org.apache.spark:spark-sql_2.12:3.3.4,'  # Spark SQL for Scala 2.12
    'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.4 '  # Kafka Streaming for Scala 2.12
    'pyspark-shell'
)

# Kafka configuration
kafka_broker = "localhost:9092"

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

def run_spark_job():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("KafkaToDelta") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
        .config("spark.sql.debug.maxToStringFields", 10000) \
        .getOrCreate()

    # Load schemas for both topics
    location_schema = load_schema("C:\\Users\\shali\\PycharmProjects\\flight-transaction-location-data-pipeline\\app\\schemas\\location_schema.json")
    transaction_schema = load_schema("C:\\Users\\shali\\PycharmProjects\\flight-transaction-location-data-pipeline\\app\\schemas\\transaction_schema.json")

    # Map topics to their respective schemas
    spark_schema_map = {
        "Location": location_schema,
        "Transaction": transaction_schema
    }

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
        #spark_schema_map = spark_schema_map,# Map of topic to schema
        kafka_bootstrap_servers=kafka_broker,
        kafka_topics=kafka_topics,  # Multiple topics
        checkpoint_path="app/checkpoint",
        output_path="app/delta_table",
        starting_offsets="earliest"
    )

    query.awaitTermination(timeout=120)
    return spark

def load_schema(schema_file_path):
    """Loads JSON schema from a file and converts it to a StructType schema."""
    with open(schema_file_path, 'r') as file:
        s = file.read()
        json_schema = json.loads(s)
        return create_spark_schema(json_schema)

def _start_kafka_stream(session, log, schema_map,kafka_bootstrap_servers, kafka_topics, checkpoint_path, output_path, starting_offsets="latest"):
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

            # Write data from the topic into Delta (you can have separate directories per topic)
            query = topic_df.writeStream \
                .format("delta") \
                .outputMode("append") \
                .option("checkpointLocation", f"{checkpoint_path}/{topic}") \
                .start(f"{output_path}/{topic}")  # You can have separate directories per topic

    return query

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

def parse_kafka_message(kafka_df, schema):
    """Parse the Kafka message value to a structured format."""
    return kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), schema).alias("data")) \
        .select("data.*")

def read_delta_table(spark, delta_table_path, num_rows=5):
    """Reads and prints the data from the Delta table."""
    if not spark:
        raise ValueError("SparkSession is not initialized.")
    print(f"Reading data from Delta table at {delta_table_path}...")
    delta_df = spark.read.format("delta").load(delta_table_path)
    delta_df.limit(num_rows).show(truncate=False)
    delta_df.printSchema()

def create_spark_schema(json_schema):
    """Converts a JSON schema to a Spark StructType."""
    fields = []
    for field_name, field_info in json_schema['properties'].items():
        spark_type = StringType()  # All fields are strings in this schema
        fields.append(StructField(field_name, spark_type, True))

    return StructType(fields)


def get_country_with_most_transactions(spark, location_delta_path, transaction_delta_path):
    """
    Fetch the country with the most transactions from the given Delta tables and print the result.

    Args:
    - spark: SparkSession instance.
    - location_delta_path: Path to the Location Delta table.
    - transaction_delta_path: Path to the Transaction Delta table.
    """
    # Register the Delta tables as temporary views
    spark.read.format("delta").load(location_delta_path).createOrReplaceTempView("Location")
    spark.read.format("delta").load(transaction_delta_path).createOrReplaceTempView("Transaction")

    # SQL query to find the country with the most transactions
    query = """
    SELECT 
        Location.CountryName,
        COUNT(Transaction.UniqueId) AS NumberOfTransactions
    FROM 
        Transaction
    JOIN 
        Location
    ON 
        Transaction.OriginAirportCode = Location.AirportCode
    GROUP BY 
        Location.CountryName
    ORDER BY 
        NumberOfTransactions DESC
    LIMIT 1
    """

    # Execute the query
    result = spark.sql(query)

    # Fetch the result as a list and access the first record
    result_list = result.collect()

    if result_list:
        # Extract the country and number of transactions
        country_name = result_list[0]["CountryName"]
        number_of_transactions = result_list[0]["NumberOfTransactions"]

        # Print the result
        print(f"The country with the most transactions is: {country_name}")
        print(f"Number of transactions: {number_of_transactions}")
    else:
        print("No results found.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    location_delta_path = "app/delta_table/Location"
    transaction_delta_path = "app/delta_table/Transaction"

    spark = run_spark_job()
    read_delta_table(spark, "app/delta_table/Location", num_rows=1000)
    get_country_with_most_transactions(spark, location_delta_path, transaction_delta_path)
