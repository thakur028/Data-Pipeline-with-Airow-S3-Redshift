# Data-Pipeline-with-Airow-S3-Redshift
Data Pipeline with Airow, S3 &amp; Redshift
#extract data from a jdbc connection and api  and then move the file to a s3 bucket and then from there to redshift.

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.jdbc.operators.jdbc import JdbcOperator
from airflow.providers.http.sensors.http import HttpSensor
from aiflow.providers.postgres import postgreshook
from airflow.providers.amazon.aws.transfers.s3 import s3hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime, timedelta
import pandas as pd
import json

def extractfromjdbc(**kwargs):
  hook = PostgresHook(postgres_conn_id = 'my_postgres_conn')
  sql = 'select * from my_table'
  df = hook.get_pandas_df(sql)
  kwargs['ti'].xcom_push(key = 'jdbc_data', value = df.to_json())

def extractfromapi(**kwargs):
  #api methods: put (update the entire table. ), patch (update a single col or row), post(send the new data to the database), get (extract data from api endpoint)
  api_hook = HttpHook(http_conn_id = 'api connection url', method='GET')
  response = api_hook.run()
  kwargs['ti'].xcom_push(key = 'api_data', value = response.text)

def loadtos3(**kwargs):
  ti = kwargs['ti']
  jdbc_data = ti.xcom_pull(task_ids = 'extract_from_jdbc', key = 'jdbc_data')
  api_data = ti.xcom_pull(task_ids = 'extract_from_api', key = 'api_data')

  s3_hook = S3Hook(aws_conn_id = 'aws_conn')
  jdbc_data.to_csv(jdbcfile)
  s3_hook.load_file(jdbcfile, 'my_bucket', 'jdbc_data.csv')
  api_data.to_csv(apifile)
  s3_hook.load_file(apifile, 'my_bucket', 'api_data.csv')

def loadtoredshift(**kwargs):
  s3_to_redshift_task = S3ToRedshiftOperator(
      task_id = 'load_to_redshift',
      schema = 'myschema',
      table = 'redshift table',
      s3bucket = 'mybucket',
      s3key = 'jdbc_data.csv',
      redshift_conn_id = 'redshift_conn',
      aws_conn_id = 'aws_conn',
      copy_options = ['csv']
  )
  s3_to_redshift_task.execute(context = kwargs)

#Define the DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 25),
    'retries': 1,}

#cron job

dag = DAG(
    default_args = default_args,
    dag_id = 'my_dag',
    schedule_interval = '@daily' # 0 0 * * * (daily midnight)
)

extract_jdbc_task = PythonOperator(
    task_id = 'extract_from_jdbc',
    python_callable = extractfromjdbc,
    dag = dag
)

extract_api_task = PythonOperator(
    task_id = 'extract_from_api',
    python_callable = extractfromapi,
    dag = dag
)

load_s3_task = PythonOperator(
    task_id = 'load_to_s3',
    python_callable = loadtos3,
    dag = dag
)

load_redshift_task = PythonOperator(
    task_id = 'load_to_redshift',
    python_callable = loadtoredshift,
    dag = dag
)

#task dependencies
#>>bitwise operators: which is used to define dependencies.

extract_jdbc_task >> load_s3_task
extract_api_task >> load_s3_task
load_s3_task >> load_redshift_task


'''
task1
task2
task3
task4

task1 >> [task2 , task3] >> task4
'''

