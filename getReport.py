import pandas as pd
from glob import glob
import numpy as np
import json
import csv

esther_channel_data_path = "/media/tnecniv/TOSHIBA EXT/esther-channel-data/"
output_csv_parent_folder_path = "./output_parent_folder.csv"
output_csv_DATA_folder_path = "./output_DATA_folder.csv"


parent_json_lst = sorted(glob(esther_channel_data_path + "*.json"))[:-1]
DATA_json_lst = sorted(glob(esther_channel_data_path + "DATA/*.json"))

with open(output_csv_DATA_folder_path, "w") as f:
    f.writelines(
        "filename,index,no. of dicts,no. of lists,no. of records,list length stats,"
        + ",".join(map(str, range(100, -1, -1)))
        + "\n"
    )
with open(output_csv_DATA_folder_path, "a") as f1:
    writer = csv.writer(f1)
    for filename in DATA_json_lst:
        with open(filename, "r", encoding="utf-8") as f2:
            in_hour_data_lst = f2.readlines()
        fn = "./" + filename.lstrip("/media/tnecniv/TOSHIBA EXT/esther-channel-data")
        writer.writerow([fn])
        for idx, in_hour_data in enumerate(in_hour_data_lst):
            data = json.loads(in_hour_data)["data"]
            dict_cnt, list_cnt, data_cnt = 0, 0, 0
            lst_len_stats = np.zeros(101, dtype=int)
            for stream in data:
                if type(stream) is dict:
                    dict_cnt += 1
                    data_cnt += 1
                elif type(stream) is list:
                    stream_len = len(stream)
                    list_cnt += 1
                    data_cnt += stream_len
                    lst_len_stats[100 - stream_len] += 1

            res = ["", idx, dict_cnt, list_cnt, data_cnt, ""]
            res.extend(lst_len_stats.tolist())
            writer.writerow(res)

pass
