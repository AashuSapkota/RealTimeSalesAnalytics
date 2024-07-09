import streamlit as st
import time
import psycopg2
from kafka import KafkaConsumer
import json
import pandas as pd
import numpy as np
from streamlit_autorefresh import st_autorefresh



def create_kafka_consumer(topic_name):
    # Set up a Kafka consumer with specified topic and configurations
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer


def fetch_sales_stats():
    conn = psycopg2.connect("host=localhost port=5000 dbname=sales user=postgres password=postgres")
    cur = conn.cursor()

    cur.execute("SELECT count(*) FROM sales")
    sales_count = cur.fetchone()[0]

    cur.execute("SELECT count(*) FROM products")
    products_count = cur.fetchone()[0]

    cur.execute("SELECT count(*) FROM categories")
    categories_count = cur.fetchone()[0]
    
    return sales_count, products_count, categories_count


def fetch_data_from_kafka(consumer):
    messages = consumer.poll(timeout_ms=1000)
    data = []
    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
    return data




def sidebar():
    if st.session_state.get('latest_update') is None:
        st.session_state['last_update'] = time.time()

    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key="auto")

    if st.sidebar.button('Refresh Data'):
        dashboard_data()



def dashboard_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last refrest at: {time.strftime('%y-%m-%d %H:%M:%S')}")

    sales_count, products_count, categories_count = fetch_sales_stats()
    
    st.markdown("------")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total sales count", sales_count)
    col2.metric("Total products count", products_count)
    col3.metric("Total categories count", categories_count)

    # Initialize or update data dictionary
    if 'data_dict' not in st.session_state:
        st.session_state['data_dict'] = {}

    for topic in topics:
        consumer = create_kafka_consumer(topic)
        data = fetch_data_from_kafka(consumer)
        
        if data:
            results = pd.DataFrame(data[0])
            st.session_state['data_dict'][topic] = results
        else:
            if topic in st.session_state['data_dict']:
                results = st.session_state['data_dict'][topic]
            else:
                results = pd.DataFrame()

        # results = pd.DataFrame(data)
        # data_dict[topic] = results

    # print("here: ", data_dict['total_sales_summary'])
    # st.text(data_dict)

     # Display the data for each topic (optional)
        st.write(f"Data for {topic}:")
        st.dataframe(results)

    # # Fetch data from Kafka on aggregated votes per candidate
    # consumer = create_kafka_consumer('total_sales_summary')
    # data = fetch_data_from_kafka(consumer)
    # print('here:', data)
    # # results = pd.DataFrame(data)
    # if data:
    #     results = pd.DataFrame(data[0])
    #     print(results)
    #     st.text(results)
    # else:
    #     st.text("No new data available")



topics = ["total_sales_summary", "sales_by_category", "sales_by_state", "sales_by_city", "customer_insights", "credit_card_usage"]


sidebar()
dashboard_data()
