import os

from dotenv import load_dotenv

BASEDIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(BASEDIR, '.env'))
APP_DICT = [

]

CREDENTIALS_JSON = os.environ.get("CREDENTIALS_JSON")
GOOGLE_SCOPE = os.environ.get("GOOGLE_SCOPE")
PROJECT_ID = os.environ.get("PROJECT_ID")
CLUSTERING_FIELD = os.environ.get("CLUSTERING_FIELD")
TIME_PARTITION_FIELD = os.environ.get("TIME_PARTITION_FIELD")
DATA_SET = os.environ.get("DATA_SET")
DATA_CHANNEL_WEBHOOKS = os.environ.get("DATA_CHANNEL_WEBHOOKS")
