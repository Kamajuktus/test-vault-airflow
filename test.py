import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime

# CONFIGURATION
# This matches the ID we stored in Vault at: airflow/connections/postgres_analytics
CONN_ID = "postgres_analytics"

logger = logging.getLogger(__name__)

def test_vault_connection():
    logger.info(f"Attempting to retrieve connection: {CONN_ID}")
    
    try:
        # This triggers the Vault Lookup
        conn = BaseHook.get_connection(CONN_ID)
        
        logger.info("Successfully retrieved connection from Vault!")
        logger.info(f"Host: {conn.host}")
        logger.info(f"Login: {conn.login}")
        logger.info(f"Schema: {conn.schema}")
        
        return "Connection retrieved successfully"
        
    except Exception as e:
        logger.error(f"Failed to retrieve connection. Ensure 'airflow/connections/{CONN_ID}' exists in Vault.")
        logger.error(f"Error details: {e}")
        raise

with DAG(
    'vault_clean_test',
    # Use a fixed date in the past (Standard Airflow Best Practice)
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['security', 'vault']
) as dag:

    t1 = PythonOperator(
        task_id='verify_vault_secret',
        python_callable=test_vault_connection
    )
    
    t1