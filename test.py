import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

# The ID we stored in Vault: airflow/connections/postgres_analytics
DB_CONNECTION_ID = "postgres_analytics"

logging.basicConfig(level=logging.INFO)

def test_operator(conn_id):
    logging.info(f"Using connection ID: {conn_id}")

with DAG(
    'vault_demo_dag',
    start_date=datetime(2026, 1, 23),
    schedule=None,
    catchup=False
) as dag:

    # Airflow automatically asks Vault for "postgres_analytics"
    t1 = PythonOperator(
        task_id='query_db_securely',
        python_callable=test_operator,
        op_args=[DB_CONNECTION_ID]
    )