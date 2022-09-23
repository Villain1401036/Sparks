

import imp
import time
from airflow import DAG 
from airflow.operators.python import PythonOperator 
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryUpsertTableOperator

from datetime import timedelta, datetime
import psycopg2


import sys



import etlfile as utils


###############################################
# Variables
###############################################
raw_table = "public.hardbounce_raw"
stg_table = "public.hardbounce_stg"
csv_source_folder = "/home/data/sparks/"
file_delimiter = ";"
bq_bucket = "sparksnet"

LocalSRC = ''
CloudDEST = ''
GCP_conn_id = ''

bq_project = ''
bq_dataset = ''

with DAG(
  'sparks',
  default_args={
 'depends_on_past': False,
        'email': ['kr7168799@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'provide_context':True,
        
        # 'sla': timedelta(hours=2),
     
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        'trigger_rule': 'all_success'
  },
  description='dag for getting 2013 posts from reddit',
    schedule_interval= "@daily",
    start_date=datetime(2022, 9, 9),
    catchup=False,
    
    tags=['example'],
                ) as dag:
    #write all the operaters that are to be used
    
    pull_users = PythonOperator(
        task_id='pull_users',
        python_callable= utils.pull_data,
        
        op_kwargs={
            "url":"https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users",
            "dest_file":"./data/users.json"
        },
        dag=dag
    )

    pull_messages = PythonOperator(
        task_id='pull_messages',
        python_callable= utils.pull_data,
        
        op_kwargs={
            "url":"https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/messages",
            "dest_file":"./data/users.json"
        },
        dag=dag
    )


    transform_users= PythonOperator(
        task_id = 'transform_users',
        python_callable= utils.transformdata_raw,
        op_kwargs={
            "src_file":'./data/users.json',
            "destfolder": "./data/",
            "table":"users",
            "dtype":{"created_utc":int,'score':int,'ups':int,'downs':int,'permalink':str,'id':str,'subreddit_id':str}
        },

    )
    transform_subs= PythonOperator(
        task_id = 'transform_subs',
        python_callable= utils.transformdata_raw,
        op_kwargs={
            "src_file":'./data/users.json',
            "destfolder": "./data/",
            "table":"subscriptions",
            "dtype":{"created_utc":int,'score':int,'ups':int,'downs':int,'permalink':str,'id':str,'subreddit_id':str}
        },

    )
    transform_messages= PythonOperator(
        task_id = 'transform_messages',
        python_callable= utils.transformdata_raw,
        op_kwargs={
            "src_file":'./data/messages.json',
            "destfolder": "./data/",
            "table":"messages",
            "dtype":{"created_utc":int,'score':int,'ups':int,'downs':int,'permalink':str,'id':str,'subreddit_id':str}
        },

    )
    
    insert_users_stg=PythonOperator(
        task_id = 'insert_users_stg',
        python_callable=utils.insert_to_postgres,
         op_kwargs={
            'conn': psycopg2.connect(database="postgres", user='rahul', password='Cherry@07', host='127.0.0.1', port='5432'),
            'src_folder':'./data/',
            'table':"users"
        },
        dag=dag
    )

    insert_subs_stg=PythonOperator(
        task_id = 'insert_subs_stg',
        python_callable=utils.insert_to_postgres,
         op_kwargs={
            'conn': psycopg2.connect(database="postgres", user='rahul', password='Cherry@07', host='127.0.0.1', port='5432'),
            'src_folder':'./data/',
            'table':"subscriptions"
        },
        dag=dag
    )

    insert_messages_stg=PythonOperator(
        task_id = 'insert_messages_stg',
        python_callable=utils.insert_to_postgres,
         op_kwargs={
            'conn': psycopg2.connect(database="postgres", user='rahul', password='Cherry@07', host='127.0.0.1', port='5432'),
            'src_folder':'./data/',
            'table':"messages"
        },
        dag=dag
    )

    export_gcs = LocalFilesystemToGCSOperator(
        task_id="export_gcs",
        src = "./data/",
        dst = CloudDEST,
        gcp_conn_id = GCP_conn_id,
        bucket=bq_bucket,
        mime_type = "csv", 
        delegate_to = "",
        dag=dag)

    gcs_to_bq_users = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq',
    bucket='cloud-samples-data',
    source_objects=['bigquery/us-states/us-states.csv'],
    destination_project_dataset_table='sparks-.gcs_to_bq_table',
    schema_fields=[
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'post_abbr', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag)

    gcs_to_bq_subscription = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq',
    bucket='cloud-samples-data',
    source_objects=['bigquery/us-states/us-states.csv'],
    destination_project_dataset_table='airflow_test.gcs_to_bq_table',
    schema_fields=[
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'post_abbr', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag)

    gcs_to_bq_message = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq',
    bucket='cloud-samples-data',
    source_objects=['bigquery/us-states/us-states.csv'],
    destination_project_dataset_table='airflow_test.gcs_to_bq_table',
    schema_fields=[
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'post_abbr', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag)


pull_users >> transform_users >> insert_users_stg 

pull_messages >> transform_messages >> insert_messages_stg 

transform_subs >> insert_subs_stg

[ insert_users_stg, insert_messages_stg ,insert_subs_stg ] >> [  gcs_to_bq_message , gcs_to_bq_subscription , gcs_to_bq_users ] 
    
