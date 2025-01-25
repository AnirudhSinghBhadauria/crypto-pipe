from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
import requests


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
     tags = ['Crypto pipeline']
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
          api = BaseHook.get_connection("crypto-poloiex")
          url = api.host
          headers = api.extra_dejson['headers']
          response = requests.get(url, headers = headers)
          
          condition = response.json().get('code') == 400
          
          print(f"\n\n{response.json()}\n\n")
          
          return PokeReturnValue(
               is_done = condition,
               xcom_value = url
          )
     
     is_api_available()

cyrpto_minute_price()