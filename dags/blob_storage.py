from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from helper.Upload_to_blob import create_ecdcraw_data
from copy_blob_to_lake import copy_blob_to_lake
from delete_source_file import delete_source_file
from data_process_uber import transform_data
from Upload_process_uber_to_SQL import Upload_process_uber_to_SQL

default_args = {
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'azure_blob_storage_dag',
    default_args=default_args,
    description='A simple Azure Blob Storage DAG',
    schedule_interval=timedelta(minutes=30),
)

# # ingest data to blob storage
# Upload_to_blob_storage_ecdc = PythonOperator(
#     task_id='ingest_data_blob_storage_ecdc',
#     python_callable=create_ecdcraw_data,
#     op_kwargs={'container_name': 'ecdcrawdata', 'json_file': 'ecdc.json'},
#     dag=dag,
# )

# # upload data to data lakes
# Copy_blob_to_lake_ecdc = PythonOperator(
#     task_id='Copy_blob_to_lake_ecdc',
#     python_callable=copy_blob_to_lake,
#     op_kwargs={'container_name': 'ecdcrawdata'},
#     dag=dag,
# )

# # delete file in blob
# delete_file_ecdc = PythonOperator(
#     task_id='delete_file_ecdc',
#     python_callable=delete_source_file,
#     op_kwargs={'container_name': 'ecdcrawdata'},
#     dag=dag,
# )

# ingest data to blob storage
Upload_to_blob_storage_uber = PythonOperator(
    task_id='ingest_data_blob_storage_uber',
    python_callable=create_ecdcraw_data,
    op_kwargs={'container_name': 'uber', 'json_file': 'uber.json'},
    dag=dag,
)

# upload data to data lakes
Copy_blob_to_lake_uber = PythonOperator(
    task_id='Copy_blob_to_lake_uber',
    python_callable=copy_blob_to_lake,
    op_kwargs={'container_name': 'uber'},
    dag=dag,
)

# delete file in blob
delete_file_uber = PythonOperator(
    task_id='delete_file_uber',
    python_callable=delete_source_file,
    op_kwargs={'container_name': 'uber'},
    dag=dag,
)

transform_data_uber = PythonOperator(
    task_id='transform_data_uber',
    python_callable=transform_data,
    op_kwargs={'container_name': 'raw', 'path_file': 'uber/uber_data.csv', 'path_folder': 'uber'},
    dag=dag,
)

upload_process_uber_to_SQL = PythonOperator(
    task_id='upload_process_uber_to_SQL',
    python_callable=Upload_process_uber_to_SQL,
    dag=dag,
)





Upload_to_blob_storage_uber >> Copy_blob_to_lake_uber >> delete_file_uber >> transform_data_uber >> upload_process_uber_to_SQL

# Upload_to_blob_storage_ecdc >> Copy_blob_to_lake_ecdc >> delete_file_ecdc


