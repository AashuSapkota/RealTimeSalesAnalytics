import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.functions import sum as _sum
import json


# Function to process each batch of data and send it to a Kafka topic
def process_batch(batch_df, batch_id, topic_name):
        # Convert the batch DataFrame to a list of dictionaries
        batch_list = batch_df.collect()
        batch_json = json.dumps([row.asDict() for row in batch_list])
        # Create a new DataFrame with the JSON string
        kafka_df = spark.createDataFrame([{'value': batch_json}], schema=StructType([StructField("value", StringType(), True)]))
        # Write the DataFrame to Kafka
        kafka_df.selectExpr("CAST(value AS STRING)").write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("topic", topic_name) \
                .save()



if __name__ == "__main__":
    # Initialize the Spark session with necessary configurations
    spark = (SparkSession.builder
             .appName("RealTimeSalesAnalysis")
             .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1')  # combine package configs # spark kafka integration
             .config('spark.jars', '/home/aashu/de_workspace/realtimesalesanalytics/postgresql-42.7.3.jar') # postgresql driver
             .config('spark.sql.adaptive.enable', 'false') # disable adaptive query execution
             .getOrCreate()
            )
    
    # Define the schema for the sales data
    sale_schema = StructType([
        StructField('first_name', StringType(), True),
        StructField('last_name', StringType(), True),
        StructField('email', StringType(), True),
        StructField('credit_card_company', StringType(), True),
        StructField('credit_card_number', StringType(), True),
        StructField('credit_card_expiration_date', StringType(), True),
        StructField('category', StringType(), True),
        StructField('product_name', StringType(), True),
        StructField('sale_price', StringType(), True),
        StructField('cost_price', StringType(), True),
        StructField('job_title', StringType(), True),
        StructField('zipcode', StringType(), True),
        StructField('state', StringType(), True),
        StructField('city', StringType(), True),
        StructField('street', StringType(), True)
    ])

    # Read the sales data from Kafka
    sales_df = (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'localhost:9092')
                .option('subscribe', 'sales_topic')
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col('value'), sale_schema).alias('data'))
                .select('data.*')
                )

    # Calculate total sales summary
    total_sales_summary_df = sales_df.withColumn('sale_price', col('sale_price').cast(FloatType())) \
                                 .withColumn('cost_price', col('cost_price').cast(FloatType())) \
                                 .agg(
                                     _sum(col('sale_price')).alias('total_sales_amount'),
                                     _sum(col('cost_price')).alias('total_cost_price'),
                                     _sum(col('sale_price') - col('cost_price')).alias('total_profit'),
                                     count('*').alias('number_of_sales')
                                 )
    
    # Calculate sales by category
    sales_by_category_df = sales_df.groupBy('category').agg(
        _sum('sale_price').alias('total_sales_amount'),
        _sum(col('sale_price') - col('cost_price')).alias('total_profit'),
        count('*').alias('number_of_sales')
    )

    # Calculate sales by state
    sales_by_state_df = sales_df.groupBy('state').agg(
        _sum('sale_price').alias('total_sales_amount'),
        _sum(col('sale_price') - col('cost_price')).alias('total_profit'),
        count('*').alias('number_of_sales')
    )

    # Calculate sales by city
    sales_by_city_df = sales_df.groupBy('city').agg(
        _sum('sale_price').alias('total_sales_amount'),
        _sum(col('sale_price') - col('cost_price')).alias('total_profit'),
        count('*').alias('number_of_sales')
    )

    # Calculate customer insights
    customer_insights_df = sales_df.groupBy('email').agg(
        count('*').alias('number_of_purchases'),
        avg('sale_price').alias('average_purchase_amount')
    )

    # Calculate credit card usage
    credit_card_usage_df = sales_df.groupBy('credit_card_company').agg(
        _sum('sale_price').alias('total_sales_amount'),
        count('*').alias('number_of_transactions')
    )

    # Mapping DataFrames to their respective Kafka topic names
    df_topic_map = {
        'total_sales_summary': total_sales_summary_df,
        'sales_by_category': sales_by_category_df,
        'sales_by_state': sales_by_state_df,
        'sales_by_city': sales_by_city_df,
        'customer_insights': customer_insights_df,
        'credit_card_usage': credit_card_usage_df
    }

    # Start queries for each DataFrame
    queries = []
    for topic_name, df in df_topic_map.items():
        foreach_batch_function = lambda batch_df, batch_id, topic=topic_name: process_batch(batch_df, batch_id, topic)
        query = (df.writeStream
                    .outputMode('update')
                    .foreachBatch(foreach_batch_function)
                    .option('checkpointLocation', f'/home/aashu/de_workspace/realtimesalesanalytics/checkpoints/{topic_name}')
                    .start()
                )
        queries.append(query)

    # Wait for the termination of all queries
    for query in queries:
        query.awaitTermination()