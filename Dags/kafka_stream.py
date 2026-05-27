from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from kafka import KafkaProducer
import time
import json
import logging

default_args = {

    'owner' : 'anuj',
    'start_date' : datetime(2026,5,20,10,00)
}


def get_data():
    response = requests.get('https://randomuser.me/api/')
    resp = response.json()['results'][0]
    return resp

def format_data(resp):
    data = {}
    location = resp['location']
    data['first_name'] = resp['name']['first']
    data['last_name'] = resp['name']['last']
    data['gender'] = resp['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}" \
                    f"{location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = resp['email']
    data['username'] = resp['login']['username']
    data['dob'] = resp['dob']['date']
    data['registered_date'] = resp['registered']['date']
    data['phone'] = resp['phone']
    data['picture'] = resp['picture']['medium']
    return data

def stream_data():
    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms = 5000)

    curr_time =time.time()
    while True:
        if time.time() > curr_time + 60:
            break
        try:
            resp = get_data()
            formatted_data = format_data(resp)
            producer.send('users-created', json.dumps(formatted_data).encode('utf-8'))
        except Exception as e:
            logging.error(e)
            continue
   
   


with DAG('user_automation', 
         default_args=default_args, 
         schedule_interval = '@daily', 
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable = stream_data)
    

