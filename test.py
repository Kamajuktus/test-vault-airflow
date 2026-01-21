import logging
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import hvac

# CONFIGURATION
VAULT_URL = "http://vault.vault.svc.cluster.local:8200"
VAULT_MOUNT_POINT = "airflow"  # The path where we enabled kv-v2
VAULT_ROLE = "airflow-role"    # The role we created in Vault
SECRET_PATH = "smtp_default"  # The specific secret path

logger = logging.getLogger(__name__)

def fetch_secret_with_k8s_token():
    # 1. Read the Kubernetes Service Account Token
    # Every K8s pod has this token mounted at this specific path by default
    token_path = '/var/run/secrets/kubernetes.io/serviceaccount/token'
    
    if not os.path.exists(token_path):
        raise FileNotFoundError(f"K8s token not found at {token_path}. Are we running in K8s?")
        
    with open(token_path, 'r') as f:
        jwt = f.read()

    # 2. Initialize the Vault Client
    client = hvac.Client(url=VAULT_URL)

    # 3. Login using the Kubernetes Auth Method
    # This exchanges the K8s JWT for a Vault Token
    try:
        client.auth.kubernetes.login(
            role=VAULT_ROLE,
            jwt=jwt
        )
    except Exception as e:
        logger.error(f"Failed to login to Vault: {e}")
        raise

    if client.is_authenticated():
        logger.info("Successfully authenticated with Vault using K8s Token!")
    else:
        raise PermissionError("Vault authentication failed.")

    # 4. Read the Secret (KV Version 2)
    try:
        response = client.secrets.kv.v2.read_secret_version(
            mount_point=VAULT_MOUNT_POINT,
            path=SECRET_PATH
        )
        
        # Extract the value
        secret_value = response['data']['data']['secret_code']
        logger.info(f"Successfully retrieved secret! The value is: {secret_value}")
        return secret_value
        
    except Exception as e:
        logger.error(f"Could not read secret: {e}")
        raise

with DAG(
    'vault_k8s_auth_demo',
    # Fixed date in the past to ensure it runs immediately
    start_date=datetime(2025, 1, 1), 
    schedule=None,
    catchup=False,
    tags=['security', 'vault']
) as dag:

    get_secret_task = PythonOperator(
        task_id='fetch_vault_secret_manually',
        python_callable=fetch_secret_with_k8s_token
    )

    get_secret_task