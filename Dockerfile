# Use an official Spark base image
FROM apache/spark-py:v3.4.0

# Install required software packages
USER root
# Install python, pip
RUN apt-get install -y  curl

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
# Add the project root to PYTHONPATH
ENV PYTHONPATH=/app

# Create a new user
RUN useradd -ms /bin/bash spark
RUN chown -R spark:spark /opt/spark/jars
USER spark

WORKDIR /opt/spark/jars

# Download dependent JAR files
RUN curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar
RUN curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar


# Create a working directory
WORKDIR /app

# Copy Spark job scripts
COPY app ./

# Copy requirements file (if using additional Python dependencies)
COPY requirements.txt ./



# Install Python dependencies
RUN pip install -r /app/requirements.txt


# Configure Spark to include necessary jars and configurations
ENV SPARK_SUBMIT_ARGS="--jars /opt/spark/jars/delta-core_2.12-2.4.0.jar"

# Set up the entrypoint to run the Spark job

#ENTRYPOINT ["/app"]

#ENTRYPOINT ["ls", "/utils"]
#
CMD ["python3", "spark_job/stream_kafka.py"]

#
##ENTRYPOINT ["ls", "/app"]
#CMD ["tail", "-f", "/dev/null"]