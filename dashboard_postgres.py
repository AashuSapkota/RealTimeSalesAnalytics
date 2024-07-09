import streamlit as st
import time
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from streamlit_autorefresh import st_autorefresh
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as _sum, col
import plotly.express as px


spark = (SparkSession.builder
             .appName("RealTimeSalesAnalysis2")
             .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1')  # combine package configs # spark kafka integration
             .config('spark.jars', '/home/aashu/de_workspace/realtimesalesanalytics/postgresql-42.7.3.jar') # postgresql driver
             .config('spark.sql.adaptive.enable', 'false') # disable adaptive query execution
             .getOrCreate()
            )
    
jdbc_url = "jdbc:postgresql://localhost:5000/sales"
connection_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}



def fetch_stats(jdbc_url, connection_properties):
    # Read tables into DataFrames
    sales_df = spark.read.jdbc(url=jdbc_url, table="sales", properties=connection_properties)
    categories_df = spark.read.jdbc(url=jdbc_url, table="categories", properties=connection_properties)
    products_df = spark.read.jdbc(url=jdbc_url, table="products", properties=connection_properties)
    customers_df = spark.read.jdbc(url=jdbc_url, table="customers", properties=connection_properties)

    # Convert cost price and sale price to numeric types
    products_df = products_df.withColumn("product_cost_price", col("product_cost_price").cast("double"))
    products_df = products_df.withColumn("product_sale_price", col("product_sale_price").cast("double"))


    sales_details = sales_df.join(products_df, sales_df.product_id == products_df.product_id)

    # Calculate total sales, total cost price, and total profit
    total_sales_price = sales_details.groupBy().agg(_sum("product_sale_price").alias("total_sales")).collect()[0]["total_sales"]
    total_cost_price = sales_details.groupBy().agg(_sum("product_cost_price").alias("total_cost_price")).collect()[0]["total_cost_price"]
    total_profit = total_sales_price - total_cost_price

    total_sales = sales_df.count()


   # Alias columns to avoid ambiguity
    sales_df = sales_df.alias("s")
    categories_df = categories_df.alias("c")
    products_df = products_df.alias("p")
    customers_df = customers_df.alias("cu")

    # Perform joins and generate reports
    sales_by_category = sales_df.join(products_df, sales_df.product_id == products_df.product_id) \
                                .join(categories_df, products_df.category_id == categories_df.category_id) \
                                .groupBy("c.category_name") \
                                .agg(count("s.sale_id").alias("total_sales"))

    sales_by_state = sales_df.join(customers_df, sales_df.customer_id == customers_df.customer_id) \
                             .groupBy("cu.customer_state") \
                             .agg(count("s.sale_id").alias("total_sales"))

    sales_by_city = sales_df.join(customers_df, sales_df.customer_id == customers_df.customer_id) \
                            .groupBy("cu.customer_city") \
                            .agg(count("s.sale_id").alias("total_sales"))

    customer_insights = sales_df.join(customers_df, sales_df.customer_id == customers_df.customer_id) \
                                .groupBy("cu.customer_id", "cu.customer_first_name", "cu.customer_last_name", "cu.customer_email", "cu.customer_job") \
                                .agg(count("s.sale_id").alias("total_purchases"))

    cc_usage_report = sales_df.join(customers_df, sales_df.customer_id == customers_df.customer_id) \
                              .groupBy("cu.customer_cc_company") \
                              .agg(count("s.sale_id").alias("total_sales"))

    # Return the DataFrames with the results
    return {
        "total_sales": total_sales,
        "total_cost_price": round(total_cost_price, 2),
        "total_profit": round(total_profit, 2),
        "sales_by_category": sales_by_category,
        "sales_by_state": sales_by_state,
        "sales_by_city": sales_by_city,
        "customer_insights": customer_insights,
        "cc_usage_report": cc_usage_report
    }

# Mapping from state names to state abbreviations
state_abbrev = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
    'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA',
    'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
    'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO',
    'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ',
    'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH',
    'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT',
    'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'
}

def plot_donut_chart(data):
    data = data.toPandas()
    labels = list(data['customer_cc_company'])
    sizes = list(data['total_sales'])
    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    ax.axis('equal')
    return fig

def plot_colored_bar_chart(results):
    results = results.toPandas()
    data_type = results['category_name']
    colors = plt.cm.viridis(np.linspace(0, 1, len(data_type)))
    plt.figure(figsize=(12, 8))  # Increase the figure size
    plt.bar(data_type, results['total_sales'], color=colors)
    plt.xlabel("Category")
    plt.ylabel("Total Sales")
    plt.title("Sales per category")
    plt.xticks(rotation=45)
    return plt

def plot_interactive_bar_chart(data, x_col, y_col, title):
    data = data.toPandas()
    fig = px.bar(data, x=x_col, y=y_col, title=title, color=y_col, height=600)
    fig.update_layout(xaxis_title=x_col, yaxis_title=y_col)
    return fig


# Function to plot choropleth map
def plot_choropleth_map(data, location_col, value_col, title):
    data = data.toPandas()
    # Map state names to abbreviations
    data['state_abbrev'] = data[location_col].map(state_abbrev)
    fig = px.choropleth(data, 
                        locations='state_abbrev', 
                        locationmode='USA-states', 
                        color=value_col, 
                        scope="usa", 
                        title=title, 
                        color_continuous_scale='Viridis')
    return fig


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
    


def update_data():
    # Placeholder to display last refresh time
    last_refresh = st.empty()
    last_refresh.text(f"Last refresh at: {time.strftime('%y-%m-%d %H:%M:%S')}")

    stats = fetch_stats(jdbc_url, connection_properties)

    st.markdown("""-----------""")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total sales count", stats["total_sales"])
    col2.metric("Total cost price", stats["total_cost_price"])
    col3.metric("Total Profit", stats["total_profit"])
    
    st.header("Card Usage Report")
    st.subheader("Distribution of Sales by Credit Card Company")
    donut_fig = plot_donut_chart(stats["cc_usage_report"])
    st.pyplot(donut_fig)

    st.markdown("""-----------""")
    
    st.header("Sales Analysis")

    st.subheader("Sales by Category")
    bar_fig = plot_colored_bar_chart(stats["sales_by_category"])
    st.pyplot(bar_fig)

    st.subheader("Sales by State")
    state_map = plot_choropleth_map(stats["sales_by_state"], 'customer_state', 'total_sales', 'Sales by State')
    st.plotly_chart(state_map)

    st.markdown("""-----------""")
    
    st.header("Customer Insights")
    st.subheader("Total Purchases by Customer")
    customer_insights_table = stats["customer_insights"].toPandas()
    st.write(customer_insights_table)

    st.markdown("""-----------""")
    
    st.header("Sales by City")
    st.subheader("Interactive Bar Chart")
    state_fig = plot_interactive_bar_chart(stats["sales_by_city"], 'customer_city', 'total_sales', 'Sales by City')
    st.plotly_chart(state_fig)



# Display sidebar
sidebar()

# Update and display data on the dashboard
update_data()
