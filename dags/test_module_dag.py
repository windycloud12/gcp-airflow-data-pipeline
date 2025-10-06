from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from modules.operators.data_processor import run_data_processing

with DAG(
    dag_id='test_module_dag',
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    tags=['module_test', 'structure'],
) as dag:
    
    process_users = PythonOperator(
        task_id='process_user_data',
        python_callable=run_data_processing,
        op_kwargs={'table_name': 'users_table'},
    )

    process_transactions = PythonOperator(
        task_id='process_transaction_data',
        python_callable=run_data_processing,
        op_kwargs={'table_name': 'transactions_table'},
    )

    process_users >> process_transactions