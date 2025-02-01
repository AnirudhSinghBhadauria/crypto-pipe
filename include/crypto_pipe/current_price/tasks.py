import ast, requests
from io import BytesIO
from airflow.hooks.base import BaseHook
from ...helpers.format import format_timestamps
from ...helpers.minio import get_minio_client
     
BUCKET_NAME = 'crypto-pipe'

def _get_current_price(url: str, ticker: str):
     api = BaseHook.get_connection("crypto-poloniex")
     response = requests.get(
          url,
          headers= api.extra_dejson['headers']
     )
     
     ticker_data = [item for item in response.json() if item['symbol'] == ticker][0]
     
     return ticker_data

def _format_prices(price_data):  
     price_data = ast.literal_eval(price_data)
     
     formatted_price_data = {
          'symbol': price_data['symbol'],
          'timestamp': format_timestamps(price_data['ts']),
          'price': price_data['price']
     }
     
     return formatted_price_data

def _store_prices(formatted_price_data):
    client = get_minio_client()         
    formatted_price_data = ast.literal_eval(formatted_price_data) 
    
    timestamp = formatted_price_data['timestamp'].replace(' ', '_')
    OBJECT_NAME = f'current-price/{formatted_price_data["symbol"]}/price.csv'

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    
    try:
        existing_data = client.get_object(BUCKET_NAME, OBJECT_NAME)
        csv_content = existing_data.data.decode('utf-8')
        
        rows = csv_content.strip().split('\n')[1:] 
        data_dict = {}
        for row in rows:
            symbol, ts, price = row.split(',')
            data_dict[ts] = (symbol, price)
    except:
        csv_content = 'symbol,timestamp,price\n'
        data_dict = {}
    
    data_dict[formatted_price_data['timestamp']] = (
        formatted_price_data['symbol'],
        formatted_price_data['price']
    )
    
    csv_content = 'symbol,timestamp,price\n'
    for ts, (symbol, price) in data_dict.items():
        csv_content += f"{symbol},{ts},{price}\n"
    
    data = BytesIO(csv_content.encode('utf-8'))
    client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=OBJECT_NAME,
        data=data,
        length=len(csv_content)
    )
    
    return f'{BUCKET_NAME}/{OBJECT_NAME}'