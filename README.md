## Automating Data Pipelines: Extracting Data from MySQL, Transforming with SQL, and Loading into Snowflake

### Introduction
<p>In today’s data-driven world, efficient data processing pipelines are critical for businesses to extract actionable insights. This article demonstrates a practical example of automating a data pipeline using Python and Apache Airflow to extract data from a MySQL database, load it into an AWS S3 bucket, transform it using SQL, and finally load the processed data into Snowflake. The complete solution leverages real-world parameters and ensures data flows smoothly from source to destination.</p>

### Prerequisites
Ensure you have the following prerequisites before you start:
1. **Python 3.7+**
2. **MySQL Database**
3. **AWS Account with S3 bucket access**
4. **Snowflake Account**
5. **Apache Airflow**

Install necessary Python packages:
```bash
pip install apache-airflow pandas mysql-connector-python boto3 snowflake-connector-python
```

### Step-by-Step Implementation

#### 1. MySQL Database Schema
Assume you have a MySQL table named `sales`:
```sql
CREATE TABLE sales (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_name VARCHAR(255),
    order_date DATE,
    amount DECIMAL(10, 2),
    status VARCHAR(50)
);
```

#### 2. Airflow DAG Definition
Create an Airflow DAG to orchestrate the data pipeline.

**airflow_dag.py**
```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import mysql.connector
import boto3
import pandas as pd
import snowflake.connector
import io

# Define S3 parameters
AWS_ACCESS_KEY_ID = 'YOUR_AWS_ACCESS_KEY_ID'
AWS_SECRET_ACCESS_KEY = 'YOUR_AWS_SECRET_ACCESS_KEY'
S3_BUCKET_NAME = 'your-s3-bucket-name'

# Define Snowflake parameters
SNOWFLAKE_USER = 'YOUR_SNOWFLAKE_USER'
SNOWFLAKE_PASSWORD = 'YOUR_SNOWFLAKE_PASSWORD'
SNOWFLAKE_ACCOUNT = 'YOUR_SNOWFLAKE_ACCOUNT'
SNOWFLAKE_WAREHOUSE = 'YOUR_SNOWFLAKE_WAREHOUSE'
SNOWFLAKE_DATABASE = 'YOUR_SNOWFLAKE_DATABASE'
SNOWFLAKE_SCHEMA = 'YOUR_SNOWFLAKE_SCHEMA'

# Define MySQL parameters
MYSQL_HOST = 'YOUR_MYSQL_HOST'
MYSQL_DATABASE = 'YOUR_MYSQL_DATABASE'
MYSQL_USER = 'YOUR_MYSQL_USER'
MYSQL_PASSWORD = 'YOUR_MYSQL_PASSWORD'

# Function to extract data from MySQL and load into S3
def extract_load_mysql_to_s3():
    # Connect to MySQL
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )
    query = "SELECT * FROM sales"
    df = pd.read_sql(query, conn)
    conn.close()

    # Convert DataFrame to CSV
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload CSV to S3
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3.put_object(Bucket=S3_BUCKET_NAME, Key='sales_data.csv', Body=csv_buffer.getvalue())

# Function to transform data
def transform_data():
    # Download CSV from S3
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key='sales_data.csv')
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))

    # Perform transformations
    df['amount'] = df['amount'] * 1.1  # Apply a 10% increase to all amounts

    # Convert DataFrame to CSV
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload transformed CSV to S3
    s3.put_object(Bucket=S3_BUCKET_NAME, Key='transformed_sales_data.csv', Body=csv_buffer.getvalue())

# Function to load data into Snowflake
def load_into_snowflake():
    # Download transformed CSV from S3
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key='transformed_sales_data.csv')
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cursor = conn.cursor()

    # Create Snowflake table if not exists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS transformed_sales (
        id INT,
        customer_name STRING,
        order_date DATE,
        amount NUMBER(10, 2),
        status STRING
    )
    """)

    # Insert DataFrame into Snowflake
    for index, row in df.iterrows():
        cursor.execute("""
        INSERT INTO transformed_sales (id, customer_name, order_date, amount, status) VALUES (%s, %s, %s, %s, %s)
        """, (row['id'], row['customer_name'], row['order_date'], row['amount'], row['status']))

    conn.close()

# Define DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'mysql_to_snowflake',
    default_args=default_args,
    description='Extract from MySQL, load into S3, transform data, and load into Snowflake',
    schedule_interval='@daily',
)

extract_load_task = PythonOperator(
    task_id='extract_load_mysql_to_s3',
    python_callable=extract_load_mysql_to_s3,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_into_snowflake',
    python_callable=load_into_snowflake,
    dag=dag,
)

# Define task dependencies
extract_load_task >> transform_task >> load_task
```

### Fina Takeways
<p>This article outlines a practical approach to building an automated data pipeline using Python and Apache Airflow.
  The example demonstrates how to extract data from a MySQL database, load it into an AWS S3 bucket, perform transformations, and finally ingest the processed data into Snowflake.</p>
.

Please refer to the respective documentation of the tools used:

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Pandas Documentation](https://pandas.pydata.org/pandas-docs/stable/)
- [Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [Snowflake Connector for Python Documentation](https://docs.snowflake.com/en/user-guide/python-connector.html)
