import json
import requests
from airflow.hooks.base import BaseHook
from minio import Minio
from io import BytesIO
from airflow.exceptions import AirflowNotFoundException
from datetime import datetime, timezone, timedelta

BUCKET_NAME = 'stock-market'

# a function that returns the client connection to the minio!
def _get_minio_client():
     minio = BaseHook.get_connection("minio")
     client = Minio(
          endpoint = minio.extra_dejson['endpoint_url'].split('//')[1],
          access_key = minio.login,
          secret_key = minio.password,
          secure = False
     )
     
     return client

# getting the stock prices using the yahoo finance api     
def _get_stock_prices(url, symbol):
     url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
     api = BaseHook.get_connection("stock_api")
     response = requests.get(
          url,
          headers = api.extra_dejson['headers']
     )
     
     return json.dumps(response.json()['chart']['result'][0])

# storing data fetched from get_stock_data into minio bucket!
def _store_prices(stock):
     client = _get_minio_client()
     
     if not client.bucket_exists(BUCKET_NAME):
          client.make_bucket(BUCKET_NAME)
     
     stock = json.loads(stock)
     symbol = stock['meta']['symbol']
     data = json.dumps(stock, ensure_ascii=False).encode('utf8')
     
     object = client.put_object(
          bucket_name = BUCKET_NAME,
          object_name = f'{symbol}/prices.json',
          data = BytesIO(data),
          length = len(data)
     )
     
     return f'{object.bucket_name}/{symbol}'

# getting the formatted prices data from the minio bucket
def _get_formatted_csv(path):
     client = _get_minio_client()
     
     prefix_name = f"{path.split("/")[1]}/formatted_prices/"
     objects = client.list_objects(
          BUCKET_NAME, 
          prefix = prefix_name, 
          recursive = True
     )
     
     for object in objects:
          if object.object_name.endswith(".csv"):
               return object.object_name
     
     raise AirflowNotFoundException(
          "The csv file doesn't exist!"
     )               

def format_timestamps(timestamp: int):
     dt_utc = datetime.fromtimestamp(timestamp/1000, timezone.utc)
     dt_ist = dt_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
     return dt_ist.strftime("%Y-%m-%d %H:%M:%S")
