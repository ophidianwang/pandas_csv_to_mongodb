"""
Examples:
python check_speed.py 5
"""

import os
import sys
from time import time
from datetime import datetime
from random import shuffle
import pandas
from pymongo import MongoClient

target_interval = sys.argv[1]

mongo_hosts = ["172.17.24.217", "172.17.24.218", "172.17.24.219"]
shuffle(mongo_hosts)

client = MongoClient(mongo_hosts, 40000)
db = client["rf1"]
collection = db["case1"]

# collection.delete_many({})

before_count = collection.count()
before_timestamp = time()
print("before insert: {0}, @ {1}".format(before_count, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

root_dir = "/home/btserver/xdr/{0}/".format(target_interval)
dir_list = os.listdir(root_dir)
print(dir_list)

metadata = {"SERVED_IMEI": str, "SERVED_MSISDN": str, "SERVED_IMSI": str}  # specify pandas dtype
csv_header = ["RECORD_TYPE", "POTENTIAL_DUPLICATE", "RECORD_SEQ_NUM", "SERVED_IMSI", "REC_OPENING_TIME",
              "SERVED_IMEI", "SERVED_MSISDN", "SERVING_NODE_ADDRESS"]
for dir_name in dir_list:
    file_dir = root_dir + dir_name + "/"
    file_list = os.listdir(file_dir)
    for filename in file_list:
        single_path = file_dir + filename
        print("working on {0}".format(single_path))
        # read lte pgw csv
        df = pandas.read_csv(single_path, engine='c', dtype=metadata, names=csv_header, parse_dates=[4])
        # add file_name, for fail over
        # df['FILE_NAME'] = pandas.Series(filename, index=df.index)
        # print(df[:3])
        # continue

        # convert and async insert
        df_dict = df.to_dict(orient="index")
        collection.insert_many(df_dict.values(), ordered=False)

after_timestamp = time()
after_count = collection.count()
print("spend {0} seconds processing csv to mongodb".format(after_timestamp - before_timestamp))
print("after insert: {0}, @ {1}".format(after_count, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))