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
![image](https://github.com/jayronsoares/frommysqltosnowflake/assets/248106/09bcfbc7-ba5d-4273-a4a4-97c65168a2ca)


<p>Please refer to the respective documentation of the tools used:</p>

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Pandas Documentation](https://pandas.pydata.org/pandas-docs/stable/)
- [Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [Snowflake Connector for Python Documentation](https://docs.snowflake.com/en/user-guide/python-connector.html)
