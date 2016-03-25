import os
from time import time
from time import mktime
from datetime import datetime
import json
from numpy.random import randint
import pandas

def mk_random_series(df, start_timestamp, end_timestamp):
    return pandas.Series(randint(start_timestamp, high=end_timestamp, size=len(df)), index=df.index) \
                .apply(datetime.fromtimestamp)

start = datetime.strptime("2015-11-05 00:00:00", "%Y-%m-%d %H:%M:%S")
end = datetime.strptime("2015-11-06 00:00:00", "%Y-%m-%d %H:%M:%S")
start_timestamp = mktime(start.timetuple())
end_timestamp = mktime(end.timetuple())

if os.name == "nt":
    file_dir = "D:\\BlueTech\PoCdata\\pgw\\20151105\\"
else:
    file_dir = "/home/vagrant/header_pgw/20151105/"
file_list = os.listdir(file_dir)
print(file_list)

# unify site_4g & site_3g
csv_site_4g = os.path.dirname(os.path.abspath(__file__)) + "/siteviewlte_etl.csv"
df_site_4g = pandas.read_csv(csv_site_4g, header=1)
df_site_4g.rename(columns={"BTS_NAME": "SITE_NAME"}, inplace=True)  # rename column BTS_NAME to SITE_NAME
df_site_4g["COUNTY"] = df_site_4g["BTS_ADDRESS"].str.slice(0,9)  # since unicode
df_site_4g["DISTRICT"] = df_site_4g["BTS_ADDRESS"].str.slice(9)  # since unicode

csv_site_3g = os.path.dirname(os.path.abspath(__file__)) + "/siteview3g_en.csv"
df_site_3g = pandas.read_csv(csv_site_3g, header=0)
df_site_3g['BAND'] = pandas.Series("NULL", index=df_site_3g.index)  # set BAND = "NULL"
df_site_3g["COUNTY"] = df_site_3g["BTS_ADDRESS"].str.slice(0,9)  # since unicode
df_site_3g["DISTRICT"] = df_site_3g["BTS_ADDRESS"].str.slice(9)  # since unicode

metadata = {"SERVED_IMEI": str, "SERVED_MSISDN": str, "SERVED_IMSI": str}  # specify pandas dtype
metadata_4g = ["TAC", "ECI", "SITE_ID", "SITE_NAME", "BAND", "COUNTY", "DISTRICT"]
metadata_3g = ["LAC", "SAC", "SITE_ID", "SITE_NAME", "BAND", "COUNTY", "DISTRICT"]

filename = file_list[0]
single_path = file_dir + filename
df = pandas.read_csv(single_path, header=0, engine='c', dtype=metadata, parse_dates=["RECORD_OPENING_TIME"])

# add file_name, for fail over
df['FILE_NAME'] = pandas.Series(filename, index=df.index)
# convert date_string to datetime object
# df['REC_OPENING_TIME'] = mk_random_series(df, start_timestamp, end_timestamp)
# join site csv by site_type
df_pgw_4g = df[df["SITE_TYPE"] == 18]
df_pgw_3g = df[df["SITE_TYPE"] == 2]
df_join_4g = df_pgw_4g.merge(df_site_4g[metadata_4g], on=["TAC", "ECI"], how="left").fillna("NULL")
df_join_3g = df_pgw_3g.merge(df_site_3g[metadata_3g], on=["SAC", "LAC"], how="left").fillna("NULL")
print(len(df_join_4g))

print(df_join_3g[:4])
print(df_join_3g[:3])