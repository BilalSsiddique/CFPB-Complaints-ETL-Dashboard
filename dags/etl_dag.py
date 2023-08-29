from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os,sys
from airflow.providers.mysql.operators.mysql import MySqlOperator

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from scripts.extract import Request_Data
from scripts.load import insert_data_into_database,write_to_gsheet
from scripts.extract import extract_data_from_database
from scripts.transform import transform_data_pandas


dag = DAG(
    dag_id= "ETL_dag_python",
    start_date=datetime(2023,8,25),
    schedule_interval=None,
    default_args={
        'retries':3,
        "retry_delay": 60
    }
)

def extract_data_from_api():
    data= Request_Data().execute()
    if data:
        return data
    else:
        return False
def load_data_in_database(**context):
    data = context['ti'].xcom_pull(task_ids= "extract_data_task")
    if data:
        print(data[0])
        resp=insert_data_into_database(data)
        print(resp)
    

def transform_and_save_data():
    db_data = extract_data_from_database()
    if db_data and isinstance(db_data, list):
        tran_data = transform_data_pandas(db_data)
        return tran_data

def load_data_in_google_sheets(**context):
    data = context['ti'].xcom_pull(task_ids= "trans_data_pandas")
    print('data after transformed',data)
    write_to_gsheet('1RdxZcVK8Fm-W9BRWC-TmannrKLy9LY9reMIJB-cx1aQ','Sheet1',data,'./scripts/cfbp-etl-2fea04d28428.json')




    
extract_task = PythonOperator(
    task_id= 'extract_data_task',
    python_callable=extract_data_from_api,
    dag= dag
)
load_task= PythonOperator(
    task_id = 'load_data_database',
    python_callable=load_data_in_database,
    dag=dag
)

transform_task= PythonOperator(
    task_id = 'trans_data_pandas',
    python_callable=transform_and_save_data,
    dag=dag
)

load_data_google_task= PythonOperator(
    task_id = 'google_sheet',
    python_callable=load_data_in_google_sheets,
    dag=dag
)



extract_task >> load_task >> transform_task >> load_data_google_task

