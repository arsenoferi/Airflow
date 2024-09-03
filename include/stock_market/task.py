from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
import requests
import json
from minio import Minio
from io import BytesIO

def _get_stock_price(url,symbol):
   url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
   api = BaseHook.get_connection('stock_api')
   response = requests.get(url, headers=api.extra_dejson['headers'])
   return json.dumps(response.json()['chart']['result'][0])

def _store_prices(stock):
    minio = BaseHook.get_connection('minio')
    client = Minio (
        endpoint = minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key = minio.login,
        secret_key = minio.password,
        secure = False
    )

    bucket_name = 'stock-market'
    
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data =  json.dumps(stock,ensure_ascii=False).encode('utf-8')
    obj = client.put_object(
        bucket_name=bucket_name,
        object_name=f"{symbol}/prices.json",
        data=BytesIO(data),
        length=len(data),
    )
    return f'{obj.bucket_name}/{symbol}'

def _get_formated_csv(path):
    minio = BaseHook.get_connection('minio')
    client = Minio (
        endpoint = minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key = minio.login,
        secret_key = minio.password,
        secure = False
    )
    
    bucket_name = 'stock-market'
    prefix_name = f"{path.split('/')[1]}/formatted_prices"
    object = client.list_objects(bucket_name, prefix=prefix_name, recursive=True)

    for obj in object:
        if obj.object_name.endswith('.csv'):
            return obj.object_name
    
    raise AirflowNotFoundException(f'No file found in {bucket_name}/{prefix_name}') 