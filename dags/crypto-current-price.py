import requests
from astro import sql as aql
from astro.files import File
from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
from astro.sql.table import Table, Metadata
from include.crypto_pipe.current_price.tasks import (
     _get_current_price,
     _format_prices,
     _store_prices
)

TICKER = 'BTC_USDT'

def on_success(context):
     return context

def on_failure(context):
     return context

@dag (
     start_date=datetime(2025, 2, 1),
     schedule = timedelta(seconds=15),
     # schedule = '@daily',
     catchup = False,
     dagrun_timeout = timedelta(seconds = 60),
     on_success_callback = on_success,
     on_failure_callback = on_failure,
     tags = ['At moment crypto price']
)
def crypto_current_price():
     @task.sensor(
          task_id = 'is_api_available',
          poke_interval = 5,
          timeout = 30,
          mode = 'poke',
          retries = 3
     )
     def is_api_available() -> PokeReturnValue:
          api = BaseHook.get_connection("crypto-poloniex")
          url = api.host
          headers = api.extra_dejson['headers']
          response = requests.get(url, headers = headers)
          condition = response.json().get('code') == 400

          return PokeReturnValue(
               is_done = condition,
               xcom_value = f"{url}/{api.extra_dejson['endpoint']}"
          )

     is_poloniex_available = is_api_available()

     get_current_price = PythonOperator(
          task_id = 'get_current_price',
          python_callable = _get_current_price,
          op_kwargs = {
               'url': '{{ task_instance.xcom_pull(task_ids = "is_api_available") }}',
               'ticker': TICKER
          },
          retries = 3
     )

     format_prices = PythonOperator(
          task_id = 'format_prices',
          python_callable = _format_prices,
          op_kwargs = {
               'price_data': '{{ task_instance.xcom_pull(task_ids = "get_current_price") }}'
          },
          retries = 3
     )

     store_prices = PythonOperator(
          task_id = "store_prices",
          python_callable = _store_prices,
          op_kwargs = {
               'formatted_price_data': '{{ task_instance.xcom_pull(task_ids = "format_prices") }}'
          },
          retries = 3
     )

     load_to_warehouse = aql.load_file(
          task_id = 'load_to_warehouse',
          input_file = File(
               path = f's3://{{{{ task_instance.xcom_pull(task_ids = "store_prices") }}}}',
               conn_id = 'crypto-minio'
          ),
          output_table = Table(
               name = f'{TICKER}_current_price',
               conn_id = 'crypto-postgres',
               metadata = Metadata(
                    schema = 'public'
               )
          ),
          if_exists = 'replace'
     )

     is_poloniex_available >> get_current_price >> format_prices >> store_prices >> load_to_warehouse

crypto_current_price()
