from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def test_mongo_connection():
    hook = MongoHook(mongo_conn_id='mongo_default')
    client = hook.get_conn()
    db = client.test
    print(f"Collections in test database: {db.list_collection_names()}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_mongo_connection',
    default_args=default_args,
    description='Test MongoDB Connection',
    schedule_interval=timedelta(days=1),
)

test_task = PythonOperator(
    task_id='test_mongo_connection',
    python_callable=test_mongo_connection,
    dag=dag,
) 