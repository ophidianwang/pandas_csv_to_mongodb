"""
Examples:
python check_speed_multi.py 5 fm01 fm02 fm03
"""

import os
import sys
import socket
from time import time
from datetime import datetime
from random import shuffle
import pandas
from pymongo import MongoClient

target_interval = sys.argv[1]
target_fms =  sys.argv[2:]

hostname = socket.gethostname()
hour = str(datetime.now().hour)
log_path = "{0}/log/check_speed_multi_{1}_{2}.log".format(
    os.path.dirname(os.path.abspath(__file__)), hostname, datetime.now().strftime("%Y%m%d%H%M"))

# mongo_hosts = ["172.17.24.217", "172.17.24.218", "172.17.24.219"]
mongo_hosts = [hostname]
shuffle(mongo_hosts)

client = MongoClient(mongo_hosts, 40000)
db = client["rf1"]
collection = db["case1"]

# collection.delete_many({})

before_count = collection.count()
before_timestamp = time()

with open(log_path, "a") as log_file:
    log_file.write("before insert: {0}, @ {1}\n".format(before_count, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

print("before insert: {0}, @ {1}".format(before_count, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

root_dir = "/home/btserver/xdr/{0}/".format(target_interval)
all_dir_list = os.listdir(root_dir)
# print(all_dir_list)
dir_list = target_fms
# print(dir_list)

metadata = {"SERVED_IMEI": str, "SERVED_MSISDN": str, "SERVED_IMSI": str}  # specify pandas dtype
csv_header = ["RECORD_TYPE", "POTENTIAL_DUPLICATE", "RECORD_SEQ_NUM", "SERVED_IMSI", "REC_OPENING_TIME",
              "SERVED_IMEI", "SERVED_MSISDN", "SERVING_NODE_ADDRESS"]
for dir_name in dir_list:
    file_dir = root_dir + dir_name + "/"
    file_list = os.listdir(file_dir)
    for filename in file_list:
        single_path = file_dir + filename
        # print("working on {0}".format(single_path))
        # read lte pgw csv
        df = pandas.read_csv(single_path, engine='c', dtype=metadata, names=csv_header, parse_dates=[4])
        # add file_name, for fail over
        # df['FILE_NAME'] = pandas.Series(filename, index=df.index)

        # convert and async insert
        df_dict = df.to_dict(orient="index")
        collection.insert_many(df_dict.values(), ordered=False)

after_timestamp = time()
after_count = collection.count()

with open(log_path, "a") as log_file:
    log_file.write("spend {0} seconds processing csv to mongodb\n".format(after_timestamp - before_timestamp))
    log_file.write("after insert: {0}, @ {1}\n".format(after_count, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

print("spend {0} seconds processing csv to mongodb".format(after_timestamp - before_timestamp))
print("after insert: {0}, @ {1}".format(after_count, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))