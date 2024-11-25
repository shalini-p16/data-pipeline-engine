# Use an official Spark base image
FROM apache/spark-py:v3.3.2

# Install required software packages
USER root


# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
# Add the project root to PYTHONPATH
ENV PYTHONPATH=/app


# Create a working directory
WORKDIR /app

# Copy Spark job scripts
COPY app ./

# Copy requirements file (if using additional Python dependencies)
COPY requirements.txt ./


# Install Python dependencies
RUN pip install -r /app/requirements.txt


# Configure Spark to include necessary jars and configurations
ENV SPARK_SUBMIT_ARGS="--jars /app/jars/delta-spark_2.12-3.2.0.jar"

# Set up the entrypoint to run the Spark job

#ENTRYPOINT ["/app"]

#ENTRYPOINT ["ls", "/utils"]
#
CMD ["python3", "spark_job/stream_kafka.py"]
#
##ENTRYPOINT ["ls", "/app"]
#CMD ["tail", "-f", "/dev/null"]