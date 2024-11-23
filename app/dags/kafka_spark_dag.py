from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
sys.path.append(r'C:\Users\shali\PycharmProjects\flight-transaction-location-data-pipeline')
from app.spark_job.process_kafka_messages import run_spark_job


# # Add the path to the separate project where Kafka producer resides
  # Adjust this path as needed
#
# # Import the Kafka producer function
# from kafka_producer import produce_messages
#
# # Your Spark job function (assuming this is already defined in your project)
# from app.spark_job import run_spark_job

# Default arguments for the DAG


default_args = {
    'owner': 'airflow',
    'retries': 1,
}


# def run_kafka_producer_task():
#     """Task to produce messages to Kafka topics."""
#     kafka_broker = 'localhost:9092'
#     transaction_messages = [
#         {"UniqueId": "12345", "TransactionDateUTC": "2023-01-01T00:00:00", "Itinerary": "MUC-CPH-ARN-MUC",
#          "OneWayOrReturn": "Return"}
#     ]
#     location_messages = [
#         {"AirportCode": "MUC", "CountryName": "Germany", "Region": "Europe"},
#         {"AirportCode": "CPH", "CountryName": "Denmark", "Region": "Europe"}
#     ]
#
#     # Produce messages for the relevant Kafka topics
#     produce_messages(kafka_broker, "flight-transaction-update", transaction_messages)
#     produce_messages(kafka_broker, "flight-location-update", location_messages)


def run_spark_job_task():
    """Task to run the Spark job for processing and storing to BigQuery."""
    run_spark_job(
        schema_registry_url='http://localhost:8081'
    )


with DAG(
        dag_id='kafka_spark_pipeline',
        default_args=default_args,
        description='Kafka to BigQuery pipeline with Kafka producer and Spark job',
        schedule_interval=None,
        start_date=days_ago(1),
        catchup=False,
) as dag:
    # # Task to produce messages to Kafka
    # kafka_producer_task = PythonOperator(
    #     task_id='run_kafka_producer',
    #     python_callable=run_kafka_producer_task
    # )

    # Task to run the Spark job
    spark_task = PythonOperator(
        task_id='run_spark_job',
        python_callable=run_spark_job_task
    )

    # Set dependencies: Kafka producer task must run before Spark job
    # kafka_producer_task >> spark_task
    spark_task
