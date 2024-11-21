import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
from pyspark.sql.types import  StructField, StringType, IntegerType, BooleanType, ArrayType, FloatType, DoubleType


# Add path to include the schema_validator module from another project
from pyspark.sql.types import StructType
import findspark
findspark.init()
import os

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.3,io.delta:delta-core_2.12:3.1.0 pyspark-shell'
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = (
    "--packages io.delta:delta-core_2.12:3.1.0 pyspark-shell"
)

# Configuration
kafka_broker = "localhost:9092"


def run_spark_job(kafka_broker):
    # Initialize Spark session
    # Create Spark session
    spark = SparkSession.builder \
        .appName("KafkaToDelta") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    with open(
            "C:\\Users\shali\PycharmProjects\\flight-transaction-location-data-pipeline\\app\schemas\location_schema.json",
            'r') as file:
        # print("xyz" + self.schema_file_path)
        s = file.read()
        json_schema = json.loads(s)
        spark_schema = create_spark_schema(json_schema)



    # Start the stream
    query = _start_kafka_stream(
        session=spark,
        log=spark._jvm.org.apache.log4j.LogManager.getLogger("KafkaToDelta"),
        schema=spark_schema,
        kafka_bootstrap_servers=kafka_broker,
        kafka_topic="location",
        checkpoint_path="app/checkpoint",
        output_path="app/delta_table"
    )

    query.awaitTermination()


def _start_kafka_stream(session, log, schema, kafka_bootstrap_servers, kafka_topic, checkpoint_path, output_path):
    log.info(f"_start_kafka_stream == {kafka_topic}")

    # Read from Kafka
    kafka_df = session.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .load()

    # Parse the JSON data
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), schema).alias("data")) \
        .select("data.*")

    # Write to Delta
    query = parsed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .start(output_path)

    return query


def convert_json_schema_to_spark(json_schema):
    """
    Convert JSON schema to Spark schema (StructType).

    :param json_schema: The JSON schema as a dictionary.
    :return: Spark schema (StructType).
    """

    def map_type(json_type):
        """ Map JSON type to Spark SQL type """
        if json_type == "string":
            return StringType()
        elif json_type == "integer":
            return IntegerType()
        elif json_type == "boolean":
            return BooleanType()
        elif json_type == "number":
            return FloatType()  # For float or integer
        elif json_type == "object":
            return StructType()  # This will be handled recursively
        elif json_type == "array":
            return ArrayType(StringType())  # Defaulting to StringType for array items
        else:
            raise ValueError(f"Unsupported JSON type: {json_type}")

    def handle_properties(properties):
        """ Recursively create fields for the properties (nested objects and arrays). """
        fields = []
        for field_name, field_details in properties.items():
            field_type = field_details["type"]

            # Handle nested objects or arrays
            if field_type == "object":
                nested_schema = convert_json_schema_to_spark(field_details)
                fields.append(StructField(field_name, nested_schema, nullable=True))
            elif field_type == "array":
                item_type = field_details["items"]["type"] if "items" in field_details else "string"
                array_type = ArrayType(map_type(item_type))
                fields.append(StructField(field_name, array_type, nullable=True))
            else:
                # Simple types (string, integer, boolean, etc.)
                fields.append(StructField(field_name, map_type(field_type), nullable=True))

        return fields


def create_spark_schema(json_schema):
    fields = []
    for field_name, field_info in json_schema['properties'].items():
        spark_type = StringType()  # All fields are strings in this schema
        fields.append(StructField(field_name, spark_type, True))

    return StructType(fields)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_spark_job(kafka_broker)