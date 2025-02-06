# https://www.elastic.co/guide/en/elasticsearch/reference/7.17/paginate-search-results.html
import argparse
import calendar
import json
import sys
import time
import requests
from pathlib import Path
import os
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from jsonpath_ng import jsonpath, parse
import logging
import glob
import re
from typing import Literal

def unix_time_string(num: int):
  x = datetime.fromtimestamp(int(num)/1000).strftime("%Y%m%d")
  return x

# Path('log.txt').touch()
# log_file = open('log.txt','w')

END_POINT = 'https://oat2.es.eu-central-1.aws.cloud.es.io'
INDICES = {
    'v1': 'follower-pisa-prod-datastore-ui-events-v1',
    'v2': 'follower-pisa-prod-datastore-ui-events-v2',
    'all': 'pisa-prod-datastore-ui-events', # v1 + v2
    'dr': 'follower-pisa-prod-datastore-delivery-results'
}
# REQUEST_SIZE = 10
args = None
pqwriter = None

def fetch_events(domain: Literal["LDW","SCI","MATHS","READ"]):

    # Setup logging
    def setup_logging():
        log_directory = os.path.join(base_dir, "logs")
        os.makedirs(log_directory, exist_ok=True)
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_name = f"{domain}_uievents_{current_datetime}.log"
        log_file_path = os.path.join(log_directory, log_file_name)
        logging.basicConfig(filename=log_file_path, filemode='a', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)

    base_dir = os.path.dirname(os.path.realpath(__file__))
    setup_logging()

    parse_arguments(
        index_key = 'all',
        # del_exec_list=del_exec_list,
        api_key = os.getenv("ELASTIC_API_KEY"),
        req_size = '5000'
    )

    
    # file_path = Path(f'./data/ui-events-{domain.tolower()}.parquet')
    file_list = glob.glob(f'./data/{domain}_results/*/*.txt')

    for delivery_ids_file_path in file_list[5:]:
        
        cnt_name = os.path.dirname(delivery_ids_file_path)[-3:]

        # delivery_ids_file_path = os.path.join(base_dir, 'data',f'{domain}_deliveryExecutionIDs.txt')

        # Read delivery_ids from a local file
        try:
            with open(delivery_ids_file_path, 'r') as file:
                delivery_ids = [line.strip() for line in file.readlines()]
            del_id_list = list(delivery_ids)
            # del_exec_list = ['31097008281#46d754750229#44e67beb9e158810c1114eb9e306476672bff9a0#18']
        except Exception as e:
            logging.error(f"Failed to read delivery_execution_ids from file: {e}")
            raise
        
        for del_id in del_id_list:
            try:
                pit_id = create_pit(args.index_key)
                raw_data_list = []
            
                hits, request_num, search_after = True, 1, None
                while hits:
                    # hits = search(args, pit_id, search_after, del_id=list(del_id.split(" ")))
                    hits = search(args, pit_id, search_after, del_id=del_id)
                    # say(f'Request {request_num} {search_after} for {del_id}; Rows {len(hits)}')
                    if hits:
                        raw_data_list.extend(save_results(hits))
                        request_num += 1
                        search_after = hits[-1]['sort']
            except Exception as e:
                logging.error(f"Failed to request API: {e}")
                raise
            finally:
                delete_pit(pit_id)

            if raw_data_list:
                df = pd.DataFrame.from_records(raw_data_list)
                df['metadata'] = df['metadata'].astype(str).apply(lambda x: re.sub(
                    r"/(data:image\\\\\/[a-zA-Z]*;base64,)[^\"]*/",
                    r"savedToS3\\",
                    x
                ))
                df['metadataRaw'] = df['metadataRaw'].astype(str).apply(lambda x: re.sub(
                    r"/(data:image\\\\\/[a-zA-Z]*;base64,)[^\"]*/",
                    r"savedToS3\\",
                    x
                ))
                # dates_list = [v for v in df.last_update_date if v is not None]
                # if(len(dates_list) > 0):
                #     most_recent_date = max(dates_list)
                # else:
                #     most_recent_date = 1716789681547

                # Prepare the output object with the variable name
                output_object = {"raw_data": raw_data_list}

                # Define folder and file names based on 'last_update_date'
                # formatted_date = unix_time_string(most_recent_date)
                folder_path = os.path.join(base_dir, 'data',f'{domain}_events', cnt_name)
                os.makedirs(folder_path, exist_ok=True)

                # file_path = os.path.join(folder_path, f"{del_id}.json")
                file_path = os.path.join(folder_path, f"{del_id}.json")
                with open(file_path, 'w', encoding='utf-8') as f_out:
                    # Ensure 'raw_data' is serialized properly; it might require custom handling depending on structure
                    json.dump(output_object, f_out, ensure_ascii=False, default=str)

        # table = pa.Table.from_pandas(df)
        # pq.write_to_dataset(table,root_path=file_path)


def parse_arguments(index_key: str, api_key: str, req_size: int) -> None:
    global args
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument('--index_key', choices=INDICES.keys(), required=True)
    # parser.add_argument('--from-date', required=True)
    # parser.add_argument('--to-date', required=True)
    # parser.add_argument('--del_exec_list', required=True)
    parser.add_argument('--api_key', required=True)
    parser.add_argument('--req_size', required=True)
    args = parser.parse_args(
        [
            '--index_key',index_key,
            # '--del_exec_list',del_exec_list,
            '--api_key',api_key,
            '--req_size',req_size
        ]
    )
    
    return args

def search(args, pit_id: str, search_after: list, del_id: str):
    # from_date = int(calendar.timegm(time.strptime(args.from_date, '%Y-%m-%d')) * 1000)
    # to_date = int(calendar.timegm(time.strptime(f'{args.to_date} 23:59:59', '%Y-%m-%d %H:%M:%S')) * 1000 + 999)
    # login_reverse = [f"{x[::-1]}.*" for x in args.login]
    # login_reverse = f"{args.login[::-1]}.*"
    body = {
        'size': int(args.req_size),
        'query': {
            "match": {
                "deliveryExecutionId": {
                    "query": del_id
                }
            }
            # "regexp": {
            #     "deliveryExecutionId": del_id
            # }
            # 'range': {
            #     'timestamp': {
            #         'from': from_date,
            #         'to': to_date,
            #
            # },
            # "terms": {
            #     "deliveryExecutionId": list(del_id)
            # }
        },
        'pit': {
            'id': pit_id,
            'keep_alive': '120m',
        },
        'sort': [
            {
                'timestamp': {
                    'order': 'asc',
                    'format': 'strict_date_optional_time_nanos',
                },
            },
        ],
    }
    if not search_after:
        body['sort'].append({'_shard_doc': 'desc'})
    else:
        body['search_after'] = search_after
        body['track_total_hits'] = False
    response = send_request('get', '/_search', body=body)
    return response['hits']['hits']

def save_results(hits: list) -> None:
    fields = [
        "_id",
        "itemId",
        "metadata",
        "tenantId",
        "batteryId",
        "messageId",
        "timestamp",
        "deliveryId",
        "responseId",
        "metadataRaw",
        "domEventType",
        "last_update_date",
        "deliveryExecutionId",
    ]

    _fieldPath = {}
    for f in fields:
        jsonpath_expression = parse(f"$.{f}")
        _fieldPath[f] = jsonpath_expression

    data = []
    for r in hits:
        row = {}
        # row = r['_source']
        # row["_id"] = r["_id"]
        # if 'last_update_date' not in row.keys():
        #     row['last_update_date'] = None
        for f in fields:
            if(f == '_id'):
                row[f] = r["_id"]
            else:
                match = _fieldPath[f].find(r["_source"])
                if(len(match) > 0):
                    row[f] =  match[0].value
                else:
                    row[f] = None

        data.append(row)
    # data = [hit['_source'] for hit in hits]

    return(data)


def create_pit(index_key: str) -> str:
    response = send_request('post', f'/{INDICES[index_key]}/_pit?keep_alive=120m')
    return response['id']


def delete_pit(pit_id: str) -> None:
    body = {'id': pit_id}
    send_request('delete', '/_pit', body=body)


def send_request(method: str, request_uri: str, /, *, params: dict=None, body: dict=None) -> dict:
    kwargs = {
        'url': f'{END_POINT}{request_uri}',
        'headers': {
            'Authorization': 'ApiKey ' + args.api_key,
        },
    }
    if params:
        kwargs['params'] = params
    if body:
        kwargs['json'] = body
    response = getattr(requests, method)(**kwargs)
    if response.status_code != 200:
        raise Exception(f'{response.status_code} => {response.text}')
    return response.json()
