from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests
import snowflake.connector

default_args = {
    'owner': 'airflow',
    'retries': 1
}

with DAG(
    dag_id='stock_data_pipeline',
    default_args=default_args,
    description='A simple stock data pipeline using Alpha Vantage and Snowflake',
    schedule_interval='@daily',  
    start_date=days_ago(1),
    catchup=False,
) as dag:

    @task
    def fetch_stock_data():
        api_key = Variable.get("alpha_vantage_api_key")
        symbol = "IBM"  
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
        
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Error fetching data: {response.text}")
        data = response.json()
        if "Time Series (Daily)" not in data:
            raise Exception("Invalid response structure from Alpha Vantage.")
        
        return data

    @task
    def process_stock_data(stock_data):
        return stock_data  

@task
def load_to_snowflake(processed_data):
    conn = None
    cursor = None
    
    try:
        conn = snowflake.connector.connect(
            user=Variable.get("snowflake_user"),
            password=Variable.get("snowflake_password"),
            account=Variable.get("snowflake_account"),
            warehouse=Variable.get("snowflake_warehouse"),
            database=Variable.get("snowflake_database"),
            schema=Variable.get("snowflake_schema")
        )
        
        cursor = conn.cursor()
        
        insert_query = "INSERT INTO your_table_name (column1, column2) VALUES (%s, %s)"  # Replace with actual table name
        
        for date, stock_info in processed_data['Time Series (Daily)'].items():
            open_price = stock_info['1. open']
            close_price = stock_info['4. close']
            
            open_price = float(open_price)
            close_price = float(close_price)
            
            cursor.execute(insert_query, (open_price, close_price))
        
        conn.commit()
        
    except Exception as e:
        raise Exception(f"Error inserting data into Snowflake: {str(e)}")
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    stock_data = fetch_stock_data()
    processed_data = process_stock_data(stock_data)
    load_to_snowflake(processed_data)
