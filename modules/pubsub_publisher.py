import json
import logging
from typing import List
import pandas as pd
from airflow.sdk import Variable
from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
from google.cloud.pubsub_v1.types import PubsubMessage

def publish_weather_data_to_pubsub(df_list: List[pd.DataFrame]) -> int:
    """
    Converts each row of the input data to a JSON message, 
    and publishes the messages in chunks to Pub/Sub.
    """
    
    # --- Config ---
    gcp_conn_id = Variable.get("gcp_conn_id")
    topic_id = Variable.get("topic_id", "weather-observations-topic")

    print(f"--- Starting publish data to Pub/Sub topic: {topic_id} ---")
    
    df = pd.concat(df_list)
    total_rows = len(df)
    logging.info(f"Total rows to publish: {total_rows}. Data shape: {df.shape}")

    # --- Pub/Sub Hook ---
    pubsub_hook = PubSubHook(gcp_conn_id=gcp_conn_id)

    CHUNK_SIZE = 5000
    print(f"Batch size: {CHUNK_SIZE}")

    total_published = 0
    
    # --- Package data and transfer format ---
    for start_idx in range(0, len(df), CHUNK_SIZE):

        end_idx = min(len(df), start_idx + CHUNK_SIZE)

        df_chunked = df.iloc[start_idx:end_idx]

        messages_dicts = []
        for _, row in df_chunked.iterrows():
            data_dict = row.to_dict()
            
            json_data = json.dumps(data_dict, default=str)
            message_data = json_data.encode('utf-8') # data must be bytes
            
            messages_dicts.append({
                'data': message_data,
            })
        
        print(f"Publishing batch {start_idx + 1} to {end_idx}...")

        pubsub_hook.publish(
            topic=topic_id,
            messages=messages_dicts
        )
        total_published = total_published + len(messages_dicts)
    
    
    print(f"Data successfully published. Total messages published: {total_published}")

    
    
