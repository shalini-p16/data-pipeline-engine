from google.cloud import bigquery

def write_to_bigquery(dataframe, table_name):
    """Write the processed dataframe to BigQuery."""
    dataframe.write.format("bigquery") \
        .option("table", table_name) \
        .option("temporaryGcsBucket", "my-temp-bucket") \
        .save()
