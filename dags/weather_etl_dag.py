from __future__ import annotations
import pendulum
from airflow.sdk import dag, task
from modules.etl_functions import (
    get_zip_download_links, 
    download_extract_and_transform, 
    load_dataframes_to_bigquery
)

@dag(
    dag_id='weather_data_etl_taskflow',
    start_date=pendulum.datetime(2025, 10, 1, tz="Asia/Taipei"),
    schedule=None,
    catchup=False,
    tags=['weather_etl', 'taskflow', 'mapping'],
    doc_md=__doc__,
    max_active_tasks=5, # Run 5 tasks at the same time.
)
def weather_etl_workflow():
    """
    Weather data ETL workflow.
    
    This workflow has three simple tasks:
    1. get_download_links: Get all links for the ZIP files.
    2. download_clean_single_file: Download and clean each file.
    3. load_to_bigquery: Combine all data and load to BigQuery.
    """
    
    # TASK 1: Get zip url to download
    zip_url_list = task(get_zip_download_links, task_id='get_download_links')()
    
    # TASK 2: Download zip files and clean data
    mapped_dataframes = task(
        download_extract_and_transform, 
        task_id='download_clean_single_file'
    ).expand(url=zip_url_list)
    
    # TASK 3: Load to BigQuery
    task(load_dataframes_to_bigquery, task_id='load_to_bigquery')(
        df_list=mapped_dataframes, staging_table_suffix="{{ ts_nodash }}")

weather_etl_workflow()
