from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook


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
def cyrpto_pipe():
     @task.sensor(
          task_id = 'is_api_available',
          poke_interval = 40,
          timeout = 300,
          mode = 'poke'
     )
     def is_api_available() -> PokeReturnValue:
          pass

cyrpto_pipe()