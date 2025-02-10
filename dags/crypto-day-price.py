from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
import requests
from include.crypto_pipe.day_price.tasks import (
     _get_per_day_price
)

SYMBOL = 'bitcoin'

def on_success(context):
     return context

def on_failure(context):
     return context

@dag (
     start_date = datetime(2025, 1, 1),
     schedule = '@daily',
     catchup = False,
     dagrun_timeout = timedelta(seconds = 60),
     on_success_callback = on_success,
     on_failure_callback = on_failure,
     tags = ['BTC per day price']
)
def crypto_day_price():
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

     get_per_day_price = PythonOperator(
          task_id = 'get_per_day_price',
          python_callable = _get_per_day_price,
          op_kwargs = {
               'url': '{{ task_instance.xcom_pulls(task_ids = "is_api_available") }}',
               'symbol': SYMBOL
          }
     )
     
     is_coincap_available >> get_per_day_price

crypto_day_price()
