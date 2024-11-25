import os

import matplotlib.pyplot as plt
from pyspark.sql.functions import col, when, count
from pyspark.sql import functions as F


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


def analyze_and_visualize_transaction_split(delta_transactions_path, delta_locations_path, spark):
    """
    Analyzes the split between domestic and international transactions
    and saves the pie chart visualization in the specified directory.

    Args:
        delta_transactions_path (str): Path to the transactions Delta table.
        delta_locations_path (str): Path to the locations Delta table.
        spark (SparkSession): Active Spark session.
    """
    # Step 1: Read the Delta tables
    transactions_df = spark.read.format("delta").load(delta_transactions_path)
    locations_df = spark.read.format("delta").load(delta_locations_path)

    # Step 2: Join transactions with locations to get country names
    origin_join = transactions_df.join(locations_df,
                                       transactions_df.OriginAirportCode == locations_df.AirportCode, "left") \
        .select("UniqueId", "DestinationAirportCode", "CountryName") \
        .withColumnRenamed("CountryName", "OriginCountry")

    complete_df = origin_join.join(locations_df,
                                   origin_join.DestinationAirportCode == locations_df.AirportCode, "left") \
        .select("UniqueId", "OriginCountry", "CountryName") \
        .withColumnRenamed("CountryName", "DestinationCountry")

    # Step 3: Classify transactions as Domestic or International
    classified_df = complete_df.withColumn(
        "TransactionType",
        when(col("OriginCountry") == col("DestinationCountry"), "Domestic").otherwise("International")
    )

    # Step 4: Count transactions by type
    result_df = classified_df.groupBy("TransactionType").agg(count("*").alias("TransactionCount"))

    # Step 5: Collect data for visualization
    result_data = result_df.collect()
    transaction_types = [row["TransactionType"] for row in result_data]
    transaction_counts = [row["TransactionCount"] for row in result_data]

    # Step 6: Visualize using Matplotlib and save the chart
    plt.figure(figsize=(8, 6))
    plt.pie(transaction_counts, labels=transaction_types, autopct='%1.1f%%', startangle=140,
            colors=["#66b3ff", "#ff9999"])
    plt.title("Domestic vs International Transactions Split")
    plt.axis('equal')  # Equal aspect ratio ensures the pie chart is a circle.

    # Create the directory if it doesn't exist
    output_dir = os.path.join("app", "utils")
    os.makedirs(output_dir, exist_ok=True)

    # Save the chart
    chart_path = os.path.join(output_dir, "transaction_split_chart.png")
    plt.savefig(chart_path)
    print(f"Pie chart saved to: {chart_path}")
    plt.close()


def get_segment_distribution(delta_transactions_path, spark):
    """
    Returns the distribution of the number of segments included in transactions.
    Each row in the transactions table represents a segment. This method will
    count the segments in each transaction and group by the transaction.

    Args:
        delta_transactions_path (str): Path to the transactions Delta table.
        spark (SparkSession): Active Spark session.

    Returns:
        DataFrame: A DataFrame with the distribution of the number of segments.
    """
    # Load the transactions Delta table
    transactions_df = spark.read.format("delta").load(delta_transactions_path)

    # Group by UniqueId (transaction ID) and count the number of segments for each transaction
    segment_count_df = transactions_df.groupBy("UniqueId").agg(F.count("SegmentNumber").alias("NumberOfSegments"))

    # Group by the number of segments to get the distribution
    segment_distribution_df = segment_count_df.groupBy("NumberOfSegments").agg(
        F.count("*").alias("NumberOfTransactions"))

    return segment_distribution_df


def visualize_segment_distribution(delta_transactions_path, spark, save_path):
    """
    Visualizes the distribution of the number of segments in transactions and saves the plot to a file.

    Args:
        delta_transactions_path (str): Path to the transactions Delta table.
        spark (SparkSession): Active Spark session.
        save_path (str): Path to save the graph.
    """
    # Get the segment distribution DataFrame
    segment_count_df = get_segment_distribution(delta_transactions_path, spark)

    # Collect the data for plotting
    segment_data = segment_count_df.collect()
    num_segments = [row["NumberOfSegments"] for row in segment_data]
    num_transactions = [row["NumberOfTransactions"] for row in segment_data]

    # Create a bar chart
    plt.figure(figsize=(10, 6))
    plt.bar(num_segments, num_transactions, color='skyblue')
    plt.xlabel("Number of Segments")
    plt.ylabel("Number of Transactions")
    plt.title("Distribution of Number of Segments in Transactions")
    plt.xticks(num_segments)

    # Save the plot to the specified directory
    os.makedirs(save_path, exist_ok=True)  # Ensure the directory exists
    save_file_path = os.path.join(save_path, "segment_distribution.png")
    plt.savefig(save_file_path)
    print(f"Graph saved to {save_file_path}")
    plt.close()  # Close the plot to free up memory



