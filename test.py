import logging
from airflow import DAG # FIXED: Correct import for Airflow 2.x
from datetime import datetime
from airflow.operators.python import PythonOperator

# The ID we stored in Vault
DB_CONNECTION_ID = "postgres_analytics"

logger = logging.getLogger(__name__)

def test_operator(conn_id):
    logger.info(f"Using connection ID: {conn_id}")
    return f"Success with {conn_id}"

with DAG(
    'vault_demo_dag',
    # FIXED: Set start_date to the past to ensure it can run immediately for testing
    start_date=datetime(2024, 1, 1), 
    schedule=None,
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='query_db_securely',
        python_callable=test_operator,
        # FIXED: op_kwargs must be a Dictionary {"arg_name": value}
        op_kwargs={"conn_id": DB_CONNECTION_ID} 
    )

    # You don't need chain() or a list for a single task, 
    # simply defining t1 inside the 'with DAG' block registers it.