import psycopg2
import random
import time
from confluent_kafka import SerializingProducer
import json
from psycopg2 import errors

# Path to the JSON file containing data
SOURCE_PATH = './random_data.json'

# Function to create the necessary tables in the PostgreSQL database
def create_tables(conn, cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS categories(
            category_id SERIAL PRIMARY KEY
            ,category_name VARCHAR(255)
        )
    """)


    cur.execute("""
        CREATE TABLE IF NOT EXISTS products(
            product_id SERIAL PRIMARY KEY
            ,product_name VARCHAR(255)
            ,product_cost_price VARCHAR(255)
            ,product_sale_price VARCHAR(255)
            ,category_id VARCHAR(255)
        )
    """)


    cur.execute("""
        CREATE TABLE IF NOT EXISTS customers(
            customer_id SERIAL PRIMARY KEY
            ,customer_first_name VARCHAR(255)
            ,customer_last_name VARCHAR(255)
            ,customer_email VARCHAR(255)
            ,customer_job VARCHAR(255)
            ,customer_cc_number VARCHAR(255)
            ,customer_cc_expiration VARCHAR(255)
            ,customer_cc_company VARCHAR(255)
            ,customer_zip_code VARCHAR(255)
            ,customer_state VARCHAR(255)
            ,customer_city VARCHAR(255)
            ,customer_street VARCHAR(255)
        )
    """)


    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales(
            sale_id SERIAL PRIMARY KEY
            ,customer_id VARCHAR(255)
            ,product_id VARCHAR(255)
        )
    """)


    conn.commit()

# Callback function for Kafka message delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


# Function to read data from a JSON file
def read_json_file():
    with open(SOURCE_PATH, 'r') as file:
        data = json.load(file)
    return data

if __name__ == "__main__":
    producer = SerializingProducer({'bootstrap.servers':'localhost:9092'})

    try:
         # Connect to the PostgreSQL database
        conn = psycopg2.connect("host=localhost port=5000 dbname=sales user=postgres password=postgres")
        cur = conn.cursor()

        create_tables(conn, cur)

        json_data = read_json_file()

        for obj in json_data:
            # Produce a message to the Kafka topic
            producer.produce(
                "sales_topic",
                key=str(obj['sale_id']),
                value=json.dumps(obj),
                on_delivery=delivery_report
            )
            print(f"Produced sales: {obj}")
            
            producer.flush() # Ensure the message is sent

            delay = random.uniform(1, 5) # Random delay between 1 to 5 seconds
            time.sleep(delay)
            
        cur.close()
        conn.close()
    except Exception as e:
        print(e)
