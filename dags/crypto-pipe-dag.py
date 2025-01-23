from airflow.decorators import task, dag
from datetime import datetime

@dag (
     start_date = datetime(2025, 1, 1),
     schedule = '@daily',
     catchup = False,
     tags = ['Crypto pipeline']
)
def cyrpto_pipe():
     pass

cyrpto_pipe()