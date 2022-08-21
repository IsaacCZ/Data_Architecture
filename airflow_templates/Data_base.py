
import airflow
import os
import psycopg2
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import timedelta
from datetime import datetime


# instantiating the Postgres Operator

default_args = {
    'owner': 'isaac',
    'depends_on_past': False,    
    'start_date': datetime(2021, 10, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}


dag = DAG('insert_data_postgres',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)


def csv_to_postgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
    curr = get_postgres_conn.cursor()
    # CSV loading to table
    with open("/behavior_analytics/data/user_purchase.csv", "r") as f:
        next(f)
        curr.copy_from(f, 'user_purchase', sep=",")
        get_postgres_conn.commit()





#Task 
task1 = PostgresOperator(task_id = 'create_table',
                        sql=
                        """
                        CREATE TABLE IF NOT EXISTS user_purchase (
                        invoice_number varchar(20),
                        stock_code varchar(20),
                        detail varchar(1000),
                        quantity bigint,
                        inovoice_date timestamp,
                        unit_price numeric(8,3),
                        customer_id varchar(50),
                        country varchar(20));
                            """,
                        postgres_conn_id= 'postgres_default', 
                        autocommit=True,
                        dag= dag)


task2 = LocalFilesystemToGCSOperator(
        task_id="upload_file_src",
        src="user_purchase.csv",
        dst="user_purchase.csv",
        bucket="mexicothisismybucket123456789mexicomexicomexico",
        dag=dag)

    )



task3 = PythonOperator(task_id='csv_to_database',
                   provide_context=True,
                   python_callable=csv_to_postgres,
                   dag=dag)




task1 >> task2 >> task3

