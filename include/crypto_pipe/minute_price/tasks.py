import requests, ast, csv
import io
from io import BytesIO
from airflow.hooks.base import BaseHook
from ...helpers.format import format_minute_datetime
from ...helpers.minio import get_minio_client

BUCKET_NAME = 'crypto-pipe'

def _get_current_price(url: str, symbol: str):
     interval = 'm1'
     api = BaseHook.get_connection("crypto-coincap")
     base_url = f'{url}{symbol}{api.extra_dejson['endpoint']}={interval}'
     
     response = requests.get(
          base_url,
          headers = api.extra_dejson['headers']
     )
     
     filtered_prices = [{
          'priceUsd': item['priceUsd'],
          'date': item['date']
     } for item in response.json()['data']]
     
     return filtered_prices

def _format_prices(raw_prices):
     price_data = ast.literal_eval(raw_prices)
     
     formatted_prices = [{
          'priceUsd': round(float(item['priceUsd']), 2),
          'date': format_minute_datetime(item['date'])
     } for item in price_data]
     
     return formatted_prices

def _store_prices(formatted_prices, symbol):
    client = get_minio_client()

    formatted_price_data = ast.literal_eval(formatted_prices)

    output = io.StringIO()  
    csv_writer = csv.writer(output)

    if formatted_price_data:
        headers = formatted_price_data[0].keys()
        csv_writer.writerow(headers)

        for price_data in formatted_price_data:
            csv_writer.writerow(price_data.values())

    csv_content = output.getvalue() 
    output.close()

    OBJECT_NAME = f'minute-price/{symbol}/price.csv'

    try:
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
    except Exception as e:
        print(f"Error checking/creating bucket: {e}")
        return

    try:
        client.remove_object(BUCKET_NAME, OBJECT_NAME)
    except Exception as e:
        print(f"Warning: Could not delete old object (may not exist yet): {e}")

    data = BytesIO(csv_content.encode('utf-8'))  
    data.seek(0)

    client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=OBJECT_NAME,
        data=data,
        length=len(data.getvalue()),  
        content_type='text/csv'
    )
    
    return f'{BUCKET_NAME}/{OBJECT_NAME}'