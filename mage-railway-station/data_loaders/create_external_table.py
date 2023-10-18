import pandas as pd
import requests
from pandas import DataFrame
from google.cloud import bigquery
from google.oauth2 import service_account
import yaml
import os  

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def get_google_credentials_from_yaml(filepath="/home/stefen/train/mage-railway-station/data_loaders/io_config.yaml"):
    with open(filepath, "r") as f:
        config = yaml.safe_load(f)

    config = config.get('default', config)

    if "GOOGLE_SERVICE_ACC_KEY_FILEPATH" not in config:
        raise KeyError("Key 'GOOGLE_SERVICE_ACC_KEY_FILEPATH' not found in configuration.")
    
    print("GOOGLE_SERVICE_ACC_KEY_FILEPATH:", config["GOOGLE_SERVICE_ACC_KEY_FILEPATH"])  

    if not os.path.exists(config["GOOGLE_SERVICE_ACC_KEY_FILEPATH"]):
        raise FileNotFoundError(f"File {config['GOOGLE_SERVICE_ACC_KEY_FILEPATH']} not found.")

    credentials = service_account.Credentials.from_service_account_file(
        config["GOOGLE_SERVICE_ACC_KEY_FILEPATH"]
    )
    return credentials

credentials = get_google_credentials_from_yaml()

project_id = ''
dataset_id = 'staging_data'
folders = ["equipment_failures", "passenger_flow", "ticket_validations", "train_departures", "incidents", "train_arrivals"]

@data_loader
def create_external_table_from_gcs():
    client = bigquery.Client(credentials=credentials, project=project_id)

    for folder in folders:
        table_id = folder
        gcs_uri = f"gs://railway_station/{folder}/*.parquet"

        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [gcs_uri]

        table = bigquery.Table(table_ref)
        table.external_data_configuration = external_config

        table = client.create_table(table, exists_ok=True)  
        print(f"Table {table_id} created in dataset {dataset_id} of project {project_id}")

@test
def test_create_external_table():
    client = bigquery.Client(credentials=credentials, project=project_id)   
    for folder in folders:
        table_id = folder
        try:
            table = client.get_table(f"{project_id}.{dataset_id}.{table_id}")
            print(f"Table {table_id} exists in dataset {dataset_id}")
        except Exception as e:
            print(f"Failed to fetch table {table_id}. Error: {str(e)}")
            raise e  

if __name__ == "__main__":
    create_external_table_from_gcs()
    test_create_external_table()
