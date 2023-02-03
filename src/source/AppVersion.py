import time

import pandas as pd
import requests
import json

from google.cloud import bigquery
from datetime import datetime

from src.actions.SlackMessage import send_messages_to_slack
from src.utils.utils import data_set_control, bq_transfer, run_bigquery
from src.config import PROJECT_ID, DATA_SET


def get_android_version(app_id: str) -> None:
    try:
        response = requests.get(f"https://play.google.com/store/apps/details?id={app_id}&hl=en_US&gl=US")
        try:
            update_date = str(response.content).split('>Updated on</div>')[1].split('>')[1].split('</')[0]

            d = datetime.strptime(update_date, '%b %d, %Y')
            update_date = d.strftime('%Y-%m-%d')
        except:
            update_date = None

        try:
            release_notes = \
                str(response.content).split('What&#39;s new')[1].split('itemprop="description">')[1].split('</div>')[0]
        except:
            release_notes = None
        data = pd.DataFrame()
        data = data.append({
            'release_notes': release_notes,
            'version_string': None,
            'app_name': app_id,
            'app_id': app_id,
            'platform': 'ANDROID',
            'release_date': f"{update_date}T00:00:00Z",
        }, ignore_index=True)
        data['release_date'] = pd.to_datetime(data['release_date'], utc=False).dt.date
        data['release_datetime'] = pd.to_datetime(data['release_date'], utc=False)

        schema = [
            bigquery.SchemaField("release_notes", "STRING"),
            bigquery.SchemaField("version_string", "STRING"),
            bigquery.SchemaField("app_name", "STRING"),
            bigquery.SchemaField("app_id", "STRING"),
            bigquery.SchemaField("platform", "STRING"),
            bigquery.SchemaField("release_date", "DATE"),
            bigquery.SchemaField("release_datetime", "DATETIME")
        ]
        if not data_set_control():
            bq_transfer(data, 'app_version', schema, 'Truncate')
        else:
            query_app_last_date = f"""
                    select * from `{PROJECT_ID}.{DATA_SET}.app_version` where app_name = '{app_id}' and platform = 'ANDROID'
                    """
            result = run_bigquery(query_app_last_date)
            if result.empty:
                send_messages_to_slack(f'NEW APP ADDED! https://play.google.com/store/apps/details?id={app_id}&hl=en_US&gl=US')
                bq_transfer(data, 'app_version', schema, 'Append')
            else:
                temp_data = data.loc[data['release_date'] > max(result['release_date'])]
                if not temp_data.empty:
                    send_messages_to_slack(f'@channel THERE IS A NEW VERSION! PLEASE CHECK https://play.google.com/store/apps/details?id={app_id}&hl=en_US&gl=US')

                    bq_transfer(data.loc[data['release_date'] > max(result['release_date'])], 'app_version',
                                          schema,
                                          'Append')
    except Exception as e:
        raise Exception(e)


def get_ios_version(app_name: str, app_id: str) -> None:
    try:
        headers = {

            'X-Apple-Store-Front': '143445-2,32',
            'apple-originating-system': 'MZStore'
        }
        for i in range(3):
            try:
                response = requests.get(f"https://apps.apple.com/us/app/{app_name}/{app_id}", headers=headers)
                str_json =str(response.content).split('versionHistory":')[1].split(']')[0] + ']'
                break
            except:
                time.sleep(3)
                str_json =str(response.content).split('versionHistory":')[1].split(']')[0] + ']'
                continue

        response_json = json.loads(
            str_json.replace("\\n", "").replace(r"\\", "").replace(r"\-", "").replace(r'\"', '"').replace(r'\x','').replace(r'\*',''))

        data = pd.DataFrame(response_json, columns=['releaseNotes', 'versionString', 'releaseDate'])
        data['app_name'] = app_name
        data['app_id'] = app_id
        data['platform'] = 'IOS'

        data = data.rename(columns={"releaseNotes": "release_notes", "versionString": "version_string"})
        data['release_date'] = pd.to_datetime(data['releaseDate'], utc=False).dt.date
        data['release_datetime'] = pd.to_datetime(data['releaseDate'], utc=False)
        del data['releaseDate']
        schema = [
            bigquery.SchemaField("release_notes", "STRING"),
            bigquery.SchemaField("version_string", "STRING"),
            bigquery.SchemaField("app_name", "STRING"),
            bigquery.SchemaField("app_id", "STRING"),
            bigquery.SchemaField("platform", "STRING"),
            bigquery.SchemaField("release_date", "DATE"),
            bigquery.SchemaField("release_datetime", "DATETIME")
        ]

        if not data_set_control():
            bq_transfer(data, 'app_version', schema, 'Truncate')
        else:
            query_app_last_date = f"""
            select * from `{PROJECT_ID}.{DATA_SET}.app_version` where app_name = '{app_name}' and platform = 'IOS'
            """
            result = run_bigquery(query_app_last_date)
            if result.empty:
                send_messages_to_slack(f'@channel NEW APP ADDED! https://apps.apple.com/at/app/{app_name}/{app_id}')

                bq_transfer(data, 'app_version', schema, 'Append')
            else:
                temp_data = data.loc[data['release_date'] > max(result['release_date'])]
                if not temp_data.empty:
                    send_messages_to_slack(f'@channel THERE IS A NEW VERSION! PLEASE CHECK https://apps.apple.com/at/app/{app_name}/{app_id}')
                    bq_transfer(data.loc[data['release_date'] > max(result['release_date'])], 'app_version',
                                          schema,
                                          'Append')
    except Exception as e:
        raise Exception(e)



