import os
from typing import List

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from src.config import CREDENTIALS_JSON, GOOGLE_SCOPE, PROJECT_ID, CLUSTERING_FIELD, TIME_PARTITION_FIELD, DATA_SET


def create_bigquery_client(project_id: str) -> bigquery:
    try:
        BASEDIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../constants"))

        credentials = service_account.Credentials.from_service_account_file(
            BASEDIR + f"/{CREDENTIALS_JSON}", scopes=[GOOGLE_SCOPE])
        credentials.with_subject('')
        bigquery_client = bigquery.Client(credentials=credentials, project=project_id)

        return bigquery_client
    except Exception as e:

        raise Exception(e)


def run_bigquery(query_bigquery: str) -> pd.DataFrame:
    try:
        bigquery_client = create_bigquery_client(PROJECT_ID)
        dataframe = bigquery_client.query(query_bigquery).to_dataframe()

        return dataframe
    except Exception as e:
        raise Exception(e)


def data_set_control() -> bool:
    try:
        client = create_bigquery_client(PROJECT_ID)
        data_sets = list(client.list_datasets())
        data_set_list = []
        for i in data_sets:
            data_set_list.append(i.full_dataset_id.split(':')[1])

        if DATA_SET in data_set_list:
            return True
        else:
            data_set_id = f"{PROJECT_ID}.{DATA_SET}"
            data_set = bigquery.Dataset(data_set_id)
            data_set.location = "europe-west3"
            client.create_dataset(data_set, timeout=50)
            return False

    except Exception as e:

        raise Exception(e)


def bq_transfer(data: pd.DataFrame, table_name: str, schema: List, write_disposition: str) -> None:
    try:
        bigquery_client = create_bigquery_client(PROJECT_ID)
        job_config = bigquery.LoadJobConfig(schema=schema)
        job_config.ignore_unknown_values = True
        job_config.autodetect = False
        job_config.source_format = bigquery.SourceFormat.CSV

        job_config.clustering_fields = [CLUSTERING_FIELD]

        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=TIME_PARTITION_FIELD,
            expiration_ms=None)

        if write_disposition == "Append":
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        table_ref = bigquery_client.dataset(DATA_SET).table(table_name)
        job = bigquery_client.load_table_from_dataframe(
            data, table_ref, job_config=job_config)
        job.result()

    except Exception as e:
        raise Exception(e)
