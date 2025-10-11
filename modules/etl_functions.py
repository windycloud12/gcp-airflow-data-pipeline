import requests
import zipfile
import io
import pandas as pd
import numpy as np
import pandas_gbq
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.sdk import Variable
from typing import List

GCP_CONN_ID = "gcp_bigquery"

NUMERIC_COLS_FOR_BQ = {
    'StationLatitude': 'NUMERIC', 
    'StationLongitude': 'NUMERIC', 
    'StnPres': 'NUMERIC', 
    'SeaPres': 'NUMERIC', 
    'AirPressure': 'NUMERIC'
}


def get_zip_download_links() -> List[str]:
    """
    TASK 1: Get download links
    
    This function should get all ZIP file URLs from a website.
    For now, it returns a fixed list for testing.
    """
    print("--- Getting ZIP file links ---")
    
    return [
        "https://history.colife.org.tw/?r=/download&path=L%2Bawo%2BixoS%2FkuK3lpK7msKPosaHnvbJf54%2B%2B5Zyo5aSp5rCj6KeA5ris5aCx5ZGKLzIwMjQxMC93ZWF0aGVyXzIwMjQxMDAxLnppcA%3D%3D"
    ]


def download_extract_and_transform(url: str) -> pd.DataFrame:
    """
    TASK 2: Download, clean, and transform data
    
    Download one ZIP file, unzip it, and clean the data.
    
    Args:
        url: A link to one ZIP file.
    Returns:
        A DataFrame.
    """
    print(f"--- Step 2.1: Downloading file from URL: {url[-50:]}... ---")

    # Download ZIP file
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error: Download failed for {url[-50:]}: {e}")
        return pd.DataFrame()

    # Unzip and read CSV in memory
    zip_data = io.BytesIO(response.content)
    df = None
    try:
        # Open ZIP file in memory
        with zipfile.ZipFile(zip_data, 'r') as zf:
            # Find CSV in ZIP
            csv_files = [name for name in zf.namelist() if name.lower().endswith('.csv')]

            if not csv_files:
                print("Error: No CSV file found in the ZIP.")
                print(f"Files in ZIP: {zf.namelist()}")
                return None
            
            # One ZIP One CSV
            csv_file_name = csv_files[0]
            print(f"--- Found CSV file: {csv_file_name} ---")

            # Read CSV
            with zf.open(csv_file_name) as csv_file:
                csv_bytes = csv_file.read()

            # Decode CSV to string
            try:
                csv_string = csv_bytes.decode('utf-8')
                encoding_used = 'UTF-8'
            except UnicodeDecodeError:
                try:
                    csv_string = csv_bytes.decode('big5')
                    encoding_used = 'Big5'
                except UnicodeDecodeError:
                    print("Error: Cannot decode CSV with UTF-8 or Big5.")
                    return None
            
            print(f"--- CSV decoded OK (used: {encoding_used}) ---")
            
            df = pd.read_csv(io.StringIO(csv_string))
            
    except zipfile.BadZipFile:
        print("Error: The downloaded file is not a valid ZIP file.")
    except Exception as e:
        print(f"An unknown error happened: {e}")

    if df is None or df.empty:
        print("Did not read DataFrame.")
        return pd.DataFrame()

    print(f"--- Step 2.2: Start cleaning {df.shape[0]} rows of data ---")
    
    # -99 is missing value, set to NaN
    df = df.replace(to_replace={"-99": np.nan, -99: np.nan, -99.0: np.nan})
    
    # Convert to datetime type
    df['phenomenonTime'] = pd.to_datetime(df['phenomenonTime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    df['Max10MinAverageWindTime'] = pd.to_datetime(df['Max10MinAverageWindTime'], errors='coerce')
    df['PeakGustTime'] = pd.to_datetime(df['PeakGustTime'], errors='coerce')
    df['DailyExtremeHighAirTemperatureTime'] = pd.to_datetime(df['DailyExtremeHighAirTemperatureTime'], errors='coerce')
    df['DailyExtremeLowAirTemperatureTime'] = pd.to_datetime(df['DailyExtremeLowAirTemperatureTime'], errors='coerce')
    
    # Change number columns to float64
    numeric_cols = list(NUMERIC_COLS_FOR_BQ.keys())
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')

    return df


def load_dataframes_to_bigquery(df_list: List[pd.DataFrame], staging_table_suffix: str):
    """
    TASK 3: Load to BigQuery

    - Combine DataFrames and remove duplicates.
    - Load data to a staging table in BQ.
    - MERGE data to make sure (StationId, phenomenonTime) is unique.

    Args:
        df_list: A list of all clean DataFrames.
        staging_table_suffix: DAG run ID.
    """
    
    # --- Config ---
    project_id = Variable.get("project_id")
    dataset_name = Variable.get("bq_dataset_name")
    table_name = Variable.get("bq_table_weather_raw_data")

    # Filter empty DataFrames and combine
    valid_dfs = [df for df in df_list if not df.empty]
    
    if not valid_dfs:
        print("Warning: No good DataFrame to load to BigQuery. Task ends.")
        return

    final_df = pd.concat(valid_dfs, ignore_index=True)

    # Remove duplicates based on StationId and phenomenonTime.
    print(f"Combined {final_df.shape[0]} rows. Start removing local duplicates...")
    final_df.drop_duplicates(subset=['StationId', 'phenomenonTime'], keep='last', inplace=True)
    
    if final_df.empty:
        print("Warning: No data to load after removing duplicates. Task ends.")
        return
        
    print(f"After removing duplicates, {final_df.shape[0]} rows are left.")

    # Table ID - Staging table and target table
    final_table_id = f"{dataset_name}.{table_name}"
    staging_table_id = f"{dataset_name}.{table_name}_staging_{staging_table_suffix}"

    print(f"--- Step 3.1: Start loading {final_df.shape[0]} rows to staging table: {staging_table_id} ---")

    # Get credentials and load to staging table
    hook = GoogleBaseHook(gcp_conn_id=GCP_CONN_ID)
    credentials = hook.get_credentials()
    
    pandas_gbq.to_gbq(
        dataframe=final_df,
        destination_table=staging_table_id,
        project_id=project_id,
        if_exists='replace',
        credentials=credentials,
        chunksize=10000,
        progress_bar=False
    )
    print(f"✅ Loaded data to staging table OK: {staging_table_id}")

    # MERGE
    print(f"--- Step 3.2: Start MERGE from {staging_table_id} to {final_table_id} ---")
    
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    
    merge_sql = f"""
    MERGE `{project_id}.{final_table_id}` AS T
    USING `{project_id}.{staging_table_id}` AS S
    ON T.StationId = S.StationId AND T.phenomenonTime = S.phenomenonTime
    WHEN NOT MATCHED THEN
      INSERT ROW
    """
    
    try:
        job = bq_hook.run(sql=merge_sql)
        print(f"✅ MERGE job finished OK! Job ID: {job}")
    except Exception as e:
        print(f"❌ MERGE job failed: {e}")
        raise

    # Delete the staging table
    print(f"--- Step 3.3: Delete staging table {staging_table_id} ---")
    client = bq_hook.get_client()
    client.delete_table(f"{project_id}.{staging_table_id}", not_found_ok=True)
    print("✅ Staging table deleted.")

    print(f"🎉 Data loaded to BigQuery table OK: {final_table_id}")