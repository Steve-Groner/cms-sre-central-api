import boto3
import os
import json

def load_secrets():
    client = boto3.client('secretsmanager', region_name='us-east-1')

    secret_names = ['rally-integration-secrets', '/snowflake/creds']
    for secret_name in secret_names:
        try:
            response = client.get_secret_value(SecretId=secret_name)
            secret = json.loads(response['SecretString'])
            for key, value in secret.items():
                varName = f"SM_{key.replace('-', '_').upper()}"
                varValue = value
                os.environ[varName] = varValue
        except Exception as e:
            print(f"Error fetching secret {secret_name}: {e}")

# Load secrets when the module is imported
load_secrets()