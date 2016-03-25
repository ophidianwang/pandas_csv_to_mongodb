"""
Examples:
python one_day.py fm01 fm02 fm03
"""

import os
import sys
from time import time
from datetime import datetime
from random import shuffle
import pandas
from pymongo import MongoClient

target_fms =  sys.argv[1:]
log_file = open("one_day_{0}.log".format("".join(target_fms)), "a")

mongo_hosts = ["172.17.24.217", "172.17.24.218", "172.17.24.219"]
shuffle(mongo_hosts)

client = MongoClient(mongo_hosts, 40000)
db = client["rf1"]
collection = db["case1"]

# collection.delete_many({})

before_count = collection.count()
before_timestamp = time()
print("before insert: {0}, @ {1}".format(before_count, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

root_dir = "/home/btserver/xdr/"
interval_list = os.listdir(root_dir)
interval_list = filter(lambda x: x.isdigit(), interval_list)
print(interval_list)
dir_list = target_fms
print(dir_list)

metadata = {"SERVED_IMEI": str, "SERVED_MSISDN": str, "SERVED_IMSI": str}  # specify pandas dtype
csv_header = ["RECORD_TYPE", "POTENTIAL_DUPLICATE", "RECORD_SEQ_NUM", "SERVED_IMSI", "REC_OPENING_TIME",
              "SERVED_IMEI", "SERVED_MSISDN", "SERVING_NODE_ADDRESS"]

for hour in xrange(24):
    for interval in interval_list:
        log_file.write("process hour #{0} minute #{1} @ {2}".format(
            hour, interval, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        print("process hour #{0} minute #{1} @ {2}".format(
            hour, interval, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        for dir_name in dir_list:
            file_dir = root_dir + str(interval) + "/" + dir_name + "/"
            file_list = os.listdir(file_dir)
            for filename in file_list:
                single_path = file_dir + filename
                # print("hour #{0}: working on {1}".format(hour ,single_path))
                # read lte pgw csv
                before_once = time()
                df = pandas.read_csv(single_path, engine='c', dtype=metadata, names=csv_header, parse_dates=[4])
                # add file_name, for fail over
                # df['FILE_NAME'] = pandas.Series(filename, index=df.index)

                # convert and async insert
                df_dict = df.to_dict(orient="index")
                collection.insert_many(df_dict.values(), ordered=False)
                after_once = time()
                row_count = len(df)
                spend_once = after_once - before_once
                insert_per_sec = row_count / spend_once

                log_file.write("spend {0} seconds on {1}, @ {2}, insert {3} rows per second\n".format(
                    spend_once, filename, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), insert_per_sec))
                print("spend {0} seconds on {1}, @ {2}, insert {3} rows per second\n".format(
                    spend_once, filename, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), insert_per_sec))

after_timestamp = time()
after_count = collection.count()

log_file.write("spend {0} seconds processing csv to mongodb".format(after_timestamp - before_timestamp))
print("spend {0} seconds processing csv to mongodb".format(after_timestamp - before_timestamp))
log_file.write("after insert: {0}, @ {1}".format(after_count, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
print("after insert: {0}, @ {1}".format(after_count, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

log_file.close()