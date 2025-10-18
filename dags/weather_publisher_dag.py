from __future__ import annotations
import pendulum
from airflow.sdk import dag, task
from modules.etl_functions import (
    get_zip_download_links, 
    download_extract_and_transform
)
from modules.pubsub_publisher import publish_weather_data_to_pubsub

@dag(
    dag_id='weather_publisher_dag',
    start_date=pendulum.datetime(2025, 10, 1, tz="Asia/Taipei"),
    schedule=None,
    catchup=False,
    tags=['weather_etl', 'taskflow', 'pubsub_provider'],
    doc_md=__doc__,
    max_active_tasks=5, # Run 5 tasks at the same time.
)
def weather_provider_workflow():
    """
    Weather data publisher workflow.
    
    This workflow has three simple tasks:
    1. get_download_links: Get all links for the ZIP files.
    2. download_clean_single_file: Download and clean each file.
    3. publish_to_pubsub: Combine all data and publish to Pub/Sub.
    """
    
    # TASK 1: Get zip url to download
    zip_url_list = task(get_zip_download_links, task_id='get_download_links')()
    
    # TASK 2: Download zip files and clean data
    mapped_dataframes = task(
        download_extract_and_transform, 
        task_id='download_clean_single_file'
    ).expand(url=zip_url_list)
    
    # TASK 3: Publish to Pub/Sub
    task(publish_weather_data_to_pubsub, task_id='publish_to_pubsub')(
        df_list=mapped_dataframes)

weather_provider_workflow()
