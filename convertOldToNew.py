from glob import glob
import numpy as np
import json
import csv
from datetime import datetime
import pandas as pd

parent_folder_path = "E:/esther-channel-data/"
DATA_folder_path = "E:/esther-channel-data/DATA/"
converted_data_folder_path = './converted_data/'    # one day per file

parent_json_paths = sorted(glob(parent_folder_path + '*.json'))[:-1]
DATA_json_paths = sorted(glob(DATA_folder_path + '*.json'))

header = ['started_at', 'ended_at', 'id', 'user_id', 'user_login', 'user_name', 'game_id', \
          'game_name', 'title', 'viewer_count', 'started_at', 'language', 'is_mature', 'tags', 'origin']
current_date = '000000'
merge_with_origin = False

if(merge_with_origin):
    pass

for parent_json in parent_json_paths:
    with open(parent_json, 'r', encoding='utf-8') as f1:
        in_hour_data_lst = f1.readlines()
    filename = parent_json.split('\\')[-1].rstrip('.json')
    date, hour = filename.split('-')
    if(date != current_date):
        current_date = date
        with open(converted_data_folder_path + f'{current_date}.csv', 'w', encoding='utf-8', newline='') as f2:
            writer = csv.writer(f2)
            writer.writerow(header)
    with open(converted_data_folder_path + f'{current_date}.csv', 'a', encoding='utf-8', newline='') as f2:
        writer = csv.writer(f2)
        for in_hour_data in in_hour_data_lst:
            data = json.loads(in_hour_data)
            streams = data['data']
            timestamp = datetime.strftime(datetime.strptime(data['time'], '%c'), '%y%m%d-%H%M%S')
            record = set()
            for stream_info in streams:
                if(type(stream_info) == dict):
                    if(stream_info['user_login'] not in record):
                        row_data = [timestamp, '', '', '']
                        row_data[2:2] = stream_info.values()
                        row_data[8] = row_data[8].replace('\n', '')
                        writer.writerow(row_data)
                        record.add(row_data[4])
                elif(type(stream_info) == list):
                    for stream in stream_info:
                        if(stream['user_login'] not in record):
                            row_data = [timestamp, '', '', '']
                            row_data[2:2] = stream.values()
                            row_data[8] = row_data[8].replace('\n', '')
                            writer.writerow(row_data)
                            record.add(row_data[4])
                else:
                    raise Exception("Weird type discovered") 
pass