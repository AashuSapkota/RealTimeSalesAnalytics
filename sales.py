import psycopg2
from confluent_kafka import Consumer, KafkaError, SerializingProducer
import json


# Configuration for Kafka broker
conf = {
    'bootstrap.servers':'localhost:9092'
}


# Create a Kafka consumer with additional configuration
consumer = Consumer(conf | {
    'group.id': 'sales-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})


# Create a Kafka producer
producer = SerializingProducer(conf)

# Function to fetch or create a category in the database
def fetch_or_create_category(conn, cur, category):
    cur.execute("""
        SELECT category_id FROM categories WHERE category_name = %s
    """, (category,))
    result = cur.fetchone()
    if result:
        return result[0]
    else:
        cur.execute("""
            INSERT INTO categories (category_name) VALUES (%s) RETURNING category_id
         """, (category,))
        conn.commit()
        new_category_id = cur.fetchone()[0]
        return new_category_id


# Function to fetch or create a product in the database
def fetch_or_create_product(conn, cur, params):
    product_name = params['product_name']
    product_sale_price = params['product_sale_price']
    product_cost_price = params['product_cost_price']
    category_id = params['category_id']

    cur.execute("""
       SELECT product_id 
       FROM products 
        WHERE product_name = %s
    """, (product_name,))
    result = cur.fetchone()
    if result:
       return result[0]
    else:
        cur.execute("""
          INSERT INTO products (product_name, product_cost_price, product_sale_price, category_id) 
          VALUES (%s,%s,%s,%s) 
          RETURNING product_id
    """, (product_name, product_cost_price, product_sale_price, category_id,))
        conn.commit()
        new_product_id = cur.fetchone()[0]
        return new_product_id


# Function to fetch or create a customer in the database
def fetch_or_create_customer(conn, cur, params):
    customer_first_name = params['first_name']
    customer_last_name = params['last_name']
    customer_email = params['email']
    customer_job = params['job_title']
    customer_cc_number = params['credit_card_number']
    customer_cc_company = params['credit_card_company']
    customer_cc_expiration = params['credit_card_expiration_date']
    customer_zip_code = params['zipcode']
    customer_state = params['state']
    customer_city = params['city']
    customer_street = params['street']

    cur.execute("""
        SELECT customer_id FROM customers 
        WHERE customer_first_name= %s 
            and customer_last_name = %s 
            and customer_email = %s
    """, (customer_first_name, customer_last_name, customer_email,))
    result = cur.fetchone()
    if result:
        return result[0]
    else:
        cur.execute("""
            INSERT INTO customers (customer_first_name, customer_last_name, customer_email, customer_job, 
                customer_cc_number, customer_cc_expiration, customer_cc_company, customer_zip_code, customer_state,
                customer_city, customer_street) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING customer_id
        """, (customer_first_name, customer_last_name, customer_email, customer_job, customer_cc_number,
              customer_cc_expiration, customer_cc_company, customer_zip_code, customer_state, customer_city, customer_street,))
        conn.commit()
        new_customer_id = cur.fetchone()[0]
        return new_customer_id

# Function to create a sale record in the database
def create_sales(conn, cur, params):
    customer_id = params['customer_id']
    product_id = params['product_id']

    cur.execute("""
        INSERT INTO sales (customer_id, product_id)
        VALUES (%s, %s)
        RETURNING sale_id
    """, (customer_id, product_id,))
    conn.commit()
    new_sales_id = cur.fetchone()[0]
    return new_sales_id




if __name__ == "__main__":
    conn = psycopg2.connect("host=localhost port=5000 dbname=sales user=postgres password=postgres")
    cur = conn.cursor()
    
    # Subscribe to the Kafka topic
    consumer.subscribe(['sales_topic'])

    try:
        while True:
            msg = consumer.poll(timeout=1.0) # Poll for messages from Kafka
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                # Decode and parse the message
                json_data = json.loads(msg.value().decode('utf-8'))
                
                category_id = fetch_or_create_category(conn, cur, json_data['category'])

                product_param = {
                    'product_name': json_data['product_name'],
                    'product_cost_price': json_data['cost_price'],
                    'product_sale_price': json_data['sale_price'],
                    'category_id': str(category_id)
                }

                product_id = fetch_or_create_product(conn, cur, product_param)

                customer_id = fetch_or_create_customer(conn, cur, json_data)

                sales_param = {
                    'customer_id': customer_id,
                    'product_id': product_id
                }
                create_sales(conn, cur, sales_param)
    except Exception as e:
        print(e)


