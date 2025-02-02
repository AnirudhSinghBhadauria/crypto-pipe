from astro import sql as aql
from astro.files import File
from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from astro.sql.table import Table, Metadata
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
import requests
from include.crypto_pipe.minute_price.tasks import (
     _get_current_price,
     _format_prices,
     _store_prices
)

SYMBOL = 'bitcoin'

def on_success(context):
     return context

def on_failure(context):
     return context

@dag (
     start_date = datetime(2025, 1, 1),
     schedule = timedelta(seconds=120),
     # schedule = '@daily',
     catchup = False,
     dagrun_timeout = timedelta(seconds = 60),
     on_success_callback = on_success,
     on_failure_callback = on_failure,
     tags = ['Per minute price (24 hours)']
)
def cyrpto_minute_price():
     @task.sensor(
          task_id = 'is_api_available',
          poke_interval = 40,
          timeout = 300,
          mode = 'poke',
          retries = 3
     )
     def is_api_available() -> PokeReturnValue:
          api = BaseHook.get_connection("crypto-coincap")
          url = api.host
          headers = api.extra_dejson['headers']
          response = requests.get(
               f"{api.host}{SYMBOL}", 
               headers = headers
          )
          
          condition = len(response.json()) == 2
          
          return PokeReturnValue(
               is_done = condition,
               xcom_value = url
          )
     
     is_coincap_available = is_api_available()

     get_per_minute_price = PythonOperator(
          task_id = 'get_per_minute_price',
          python_callable = _get_current_price,
          op_kwargs = {
               'url': '{{ task_instance.xcom_pull(task_ids = "is_api_available") }}',
               'symbol': SYMBOL
          }
     )
     
     format_prices = PythonOperator(
          task_id = 'format_prices',
          python_callable = _format_prices,
          op_kwargs = {
               'raw_prices': '{{ task_instance.xcom_pull(task_ids = "get_per_minute_price") }}'
          }
     )
     
     store_prices = PythonOperator(
          task_id = 'store_prices',
          python_callable = _store_prices,
          op_kwargs = {
               'formatted_prices': '{{ task_instance.xcom_pull(task_ids = "format_prices") }}',
               'symbol': SYMBOL
          }
     )
     
     load_to_warehouse = aql.load_file(
          task_id = 'load_to_warehouse',
          input_file = File(
               path = f's3://{{{{ task_instance.xcom_pull(task_ids = "store_prices") }}}}',
               conn_id = 'crypto-minio'
          ),
          output_table = Table(
               name = f'{SYMBOL}_minute_price',
               conn_id = 'crypto-postgres',
               metadata = Metadata(
                    schema = 'public'
               )
          ),
          if_exists = 'replace'    
     )        
     
     is_coincap_available >> get_per_minute_price >> format_prices >> store_prices >> load_to_warehouse
     
cyrpto_minute_price()
